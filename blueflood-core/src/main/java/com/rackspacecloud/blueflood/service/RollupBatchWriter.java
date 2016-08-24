/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.service;

import com.datastax.driver.core.TokenRange;
import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.types.RollupType;
import org.apache.cassandra.thrift.KeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;

// Batches rollup writes
public class RollupBatchWriter {
    private final Logger LOG = LoggerFactory.getLogger(RollupBatchWriter.class);
    private final AbstractMetricsRW basicMetricsRW;
    private final AbstractMetricsRW preAggregatedRW;
    private final ThreadPoolExecutor executor;
    private final RollupExecutionContext context;

    private final Map<TokenRange, ConcurrentLinkedQueue<SingleRollupWriteContext>> locatorMap = new ConcurrentHashMap<TokenRange, ConcurrentLinkedQueue<SingleRollupWriteContext>>();
    private static final int ROLLUP_BATCH_MIN_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.ROLLUP_BATCH_MIN_SIZE);
    private static final int ROLLUP_BATCH_MAX_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.ROLLUP_BATCH_MAX_SIZE);

    public RollupBatchWriter(ThreadPoolExecutor executor, RollupExecutionContext context) {
        this.executor = executor;
        this.context = context;
        this.basicMetricsRW = IOContainer.fromConfig().getBasicMetricsRW();
        this.preAggregatedRW = IOContainer.fromConfig().getPreAggregatedMetricsRW();
    }


    public synchronized void enqueueRollupForWrite(SingleRollupWriteContext rollupWriteContext) {

        if (locatorMap.get(rollupWriteContext.getTokenRange()) != null) {
            ConcurrentLinkedQueue<SingleRollupWriteContext> rollupQueue1 = locatorMap.get(rollupWriteContext.getTokenRange());
            rollupQueue1.add(rollupWriteContext);
        } else {
            ConcurrentLinkedQueue<SingleRollupWriteContext> rollupQueue1 = new ConcurrentLinkedQueue<SingleRollupWriteContext>();
            rollupQueue1.add(rollupWriteContext);
            locatorMap.put(rollupWriteContext.getTokenRange(), rollupQueue1);
        }

        context.incrementWriteCounter();
        // enqueue MIN_SIZE batches only if the threadpool is unsaturated.
        // else, enqueue when we have >= MAX_SIZE pending

        for (Map.Entry<TokenRange, ConcurrentLinkedQueue<SingleRollupWriteContext>> locatorEntry: locatorMap.entrySet()) {
            if (locatorEntry.getValue().size() >= ROLLUP_BATCH_MIN_SIZE) {
                if (executor.getActiveCount() < executor.getPoolSize() ||
                        locatorEntry.getValue().size() >= ROLLUP_BATCH_MAX_SIZE) {
                    drainBatch(locatorEntry.getKey());
                }
            }
        }
    }


    public synchronized void drainBatch(TokenRange tokenRange) {
        List<SingleRollupWriteContext> writeBasicContexts = new ArrayList<SingleRollupWriteContext>();
        List<SingleRollupWriteContext> writePreAggrContexts = new ArrayList<SingleRollupWriteContext>();
        try {
            ConcurrentLinkedQueue<SingleRollupWriteContext> rollupQueue2 = locatorMap.get(tokenRange);
            for (int i=0; i<=ROLLUP_BATCH_MAX_SIZE; i++) {
                SingleRollupWriteContext context = rollupQueue2.remove();
                if ( context.getRollup().getRollupType() == RollupType.BF_BASIC ) {
                    writeBasicContexts.add(context);
                } else {
                    writePreAggrContexts.add(context);
                }
            }
        } catch (NoSuchElementException e) {
            // pass
        }
        if (writeBasicContexts.size() > 0) {
            LOG.debug(
                    String.format("drainBatch(): kicking off RollupBatchWriteRunnables for %d basic contexts",
                            writeBasicContexts.size()));
            executor.execute(new RollupBatchWriteRunnable(writeBasicContexts, context, basicMetricsRW));
        }
        if (writePreAggrContexts.size() > 0) {
            LOG.debug(
                    String.format("drainBatch(): kicking off RollupBatchWriteRunnables for %d preAggr contexts",
                            writePreAggrContexts.size()));
            executor.execute(new RollupBatchWriteRunnable(writePreAggrContexts, context, preAggregatedRW));
        }
    }


    public synchronized void drainBatch() {
        for (Map.Entry<TokenRange, ConcurrentLinkedQueue<SingleRollupWriteContext>> locatorEntry: locatorMap.entrySet()) {
            if (locatorEntry.getValue() != null && locatorEntry.getValue().size() > 0) {
                //From the remaining locators draining one token range at a time
                drainBatch(locatorEntry.getKey());
            }
        }
    }
}
