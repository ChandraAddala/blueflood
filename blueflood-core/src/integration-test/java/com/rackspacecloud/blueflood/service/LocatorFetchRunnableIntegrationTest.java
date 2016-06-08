package com.rackspacecloud.blueflood.service;

import com.codahale.metrics.Counter;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.inputs.processors.BatchWriter;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.io.MetricsRW;
import com.rackspacecloud.blueflood.io.ShardStateIO;
import com.rackspacecloud.blueflood.io.astyanax.ABasicMetricsRW;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxMetricsWriter;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class LocatorFetchRunnableIntegrationTest  extends IntegrationTestBase {

    private static final int MAX_ROLLUP_READ_THREADS = Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS);
    private static final int MAX_ROLLUP_WRITE_THREADS = Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_WRITE_THREADS);
    private static int WRITE_THREADS = Configuration.getInstance().getIntegerProperty(CoreConfig.METRICS_BATCH_WRITER_THREADS);
    private static TimeValue DEFAULT_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);

    private final ShardStateIO io = IOContainer.fromConfig().getShardStateIO();
    private BatchWriter batchWriter;

    private ScheduleContext ingestionCtx;
    private ShardStateWorker ingestPuller;
    private ShardStateWorker ingestPusher;

    private ScheduleContext rollupCtx;
    private ShardStateWorker rollupPuller;
    private ShardStateWorker rollupPusher;

    private ExecutorService rollupReadExecutor;
    private ThreadPoolExecutor rollupWriteExecutor;
    private ExecutorService enumValidatorExecutor;
    private LocatorFetchRunnable lfr;

    private final Clock mockClock = mock(Clock.class);

    private final int TEST_SHARD = 1;
    private final int TEST_SLOT = 4;
    private final String TENANT_ID = "983347";

    private final long REF_TIME = 1234000L; //start time of slot 4 for metrics_5m

    private final long ROLLUP_DELAY_MILLIS = 300000;
    private final long SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS = 600000;
    private final long LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS = 500000;
    private final long MIN_5_IN_MILLIS = 5 * 60 * 1000;

    private List<Integer> shards = new ArrayList<Integer>(){{
        add(1);
        add(2);
    }};

    protected MetricsRW astyanaxMetricsRW = new ABasicMetricsRW();

    @Before
    public void setup() {

        final ShardStateIO io = IOContainer.fromConfig().getShardStateIO();

        ingestionCtx = new ScheduleContext(REF_TIME, shards, mockClock);
        rollupCtx = new ScheduleContext(REF_TIME, shards, mockClock);

        // Shard workers for rollup ctx
        rollupPuller = new ShardStatePuller(shards, rollupCtx.getShardStateManager(), this.io);
        rollupPusher = new ShardStatePusher(shards, rollupCtx.getShardStateManager(), this.io);

        // Shard workers for ingest ctx
        ingestPuller = new ShardStatePuller(shards, ingestionCtx.getShardStateManager(), this.io);
        ingestPusher = new ShardStatePusher(shards, ingestionCtx.getShardStateManager(), this.io);

        batchWriter = new BatchWriter(
                new ThreadPoolBuilder()
                        .withName("Metric Batch Writing")
                        .withCorePoolSize(WRITE_THREADS)
                        .withMaxPoolSize(WRITE_THREADS)
                        .withSynchronousQueue()
                        .build(),
                new AstyanaxMetricsWriter(),
                DEFAULT_TIMEOUT,
                mock(Counter.class),
                ingestionCtx
        );

        rollupReadExecutor = new ThreadPoolExecutor(MAX_ROLLUP_READ_THREADS, MAX_ROLLUP_READ_THREADS, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

        rollupWriteExecutor = new ThreadPoolExecutor(MAX_ROLLUP_WRITE_THREADS, MAX_ROLLUP_WRITE_THREADS, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
    }

    @Test
    public void testProcessingAllLocatorsForASlot() throws Exception {

        //metrics for slot 4
        final Map<Integer, List<IMetric>> metricsShardMap = generateMetrics(REF_TIME + 1);

        //inserting metrics corresponding to slot 4 for shards 1 and 2
        ArrayList<List<IMetric>> input = new ArrayList<List<IMetric>>();
        for (int shard: shards) {
            input.add(metricsShardMap.get(shard));
        }

        when(mockClock.now()).thenReturn(new Instant(REF_TIME + 2));
        ListenableFuture<List<Boolean>> futures = batchWriter.apply(input);
        futures.get(DEFAULT_TIMEOUT.getValue(), DEFAULT_TIMEOUT.getUnit());

        ingestPusher.performOperation(); // Shard state is persisted on ingestion host

        rollupPuller.performOperation(); // Shard state is read on rollup host
        rollupCtx.setCurrentTimeMillis(REF_TIME + ROLLUP_DELAY_MILLIS + 10);
        rollupCtx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        when(mockClock.now()).thenReturn(new Instant(REF_TIME + ROLLUP_DELAY_MILLIS));
        List<SlotKey> scheduledSlotKeys = new ArrayList<SlotKey>();
        while (rollupCtx.hasScheduled()) {
            SlotKey slotKey = rollupCtx.getNextScheduled();
            scheduledSlotKeys.add(slotKey);

            LocatorFetchRunnable locatorFetchRunnable = spy(new LocatorFetchRunnable(rollupCtx, slotKey, rollupReadExecutor, rollupWriteExecutor, enumValidatorExecutor));

            RollupExecutionContext rollupExecutionContext = spy(new RollupExecutionContext(Thread.currentThread()));
            RollupBatchWriter rollupBatchWriter = spy(new RollupBatchWriter(rollupWriteExecutor, rollupExecutionContext));
            when(locatorFetchRunnable.createRollupExecutionContext()).thenReturn(rollupExecutionContext);
            when(locatorFetchRunnable.createRollupBatchWriter(any(RollupExecutionContext.class))).thenReturn(rollupBatchWriter);

            locatorFetchRunnable.run();

            //verifying number of locators read and rollups written are same as the number of locators ingested per shard.
            verify(rollupExecutionContext, times(metricsShardMap.get(slotKey.getShard()).size())).incrementReadCounter();
            verify(rollupBatchWriter, times(metricsShardMap.get(slotKey.getShard()).size())).enqueueRollupForWrite(any(SingleRollupWriteContext.class));

            rollupCtx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS, LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        }
        assertEquals("Invalid number of slots scheduled", 10, scheduledSlotKeys.size()); //5 for each shard

        for (SlotKey slotKey : scheduledSlotKeys) {
            UpdateStamp updateStamp = rollupCtx.getShardStateManager().getUpdateStamp(slotKey);

            assertEquals("Slot should be in rolled state", UpdateStamp.State.Rolled, updateStamp.getState());
            assertTrue("last rollup time stamp not updated", updateStamp.getLastRollupTimestamp() > 0);
        }

    }

    private Map<Integer, List<IMetric>> generateMetrics(final long collectionTime) {
        Map<Integer, List<IMetric>> metricMap = new HashMap<Integer, List<IMetric>>();

        //locator names and tenant id are chosen so that <tenantid>.<locator> belongs to shards 1 and 2.

        //locators corresponding got shard 1
        metricMap.put(1, new ArrayList<IMetric>() {{
            add(generateMetric(TENANT_ID, "bandwidthOut_cdn3.powerblocktv.com_APAC", collectionTime));
            add(generateMetric(TENANT_ID, "bandwidthOut_cdn5.powernationtv.com_APAC", collectionTime + 1));
        }});

        //locators corresponding got shard 2
        metricMap.put(2, new ArrayList<IMetric>(){{
            add(generateMetric(TENANT_ID, "requestCount_cdn4.powernationtv.com_EMEA", collectionTime + 2));
            add(generateMetric(TENANT_ID, "requestCount_cdn4.powernationtv.com_None", collectionTime + 3));
            add(generateMetric(TENANT_ID, "requestCount_cdn4.powerblocktv.com_Australia", collectionTime + 4));
        }});

        return metricMap;
    }

    private Metric generateMetric(String tenantId, String locator, long collectionTime) {
        return new Metric( Locator.createLocatorFromDbKey(String.format("%s.%s", tenantId, locator)),
                System.currentTimeMillis() % 100,
                collectionTime,
                new TimeValue(1, TimeUnit.DAYS),
                "unit" );
    }

}
