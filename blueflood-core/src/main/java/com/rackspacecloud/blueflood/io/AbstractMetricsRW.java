/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.blueflood.io;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.rackspacecloud.blueflood.cache.CombinedTtlProvider;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.cache.TenantTtlProvider;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This is base class of all MetricsRW classes that deals with persisting/reading
 * data from metrics_string, metrics_{granularity} and metrics_preaggregated_{granularity} column
 * family. This contains some utility methods used/shared amongst various
 * implementation/subclasses of MetricsRW.
 */
public abstract class AbstractMetricsRW implements MetricsRW {

    protected static final String DATA_TYPE_CACHE_KEY = MetricMetadata.TYPE.toString().toLowerCase();

    protected static TenantTtlProvider TTL_PROVIDER = CombinedTtlProvider.getInstance();

    // this collection is used to reduce the number of locators that get written.
    // Simply, if a locator has been seen within the last 10 minutes, don't bother.
    protected static final Cache<String, Boolean> insertedLocators =
            CacheBuilder.newBuilder().expireAfterAccess(10,
                        TimeUnit.MINUTES).concurrencyLevel(16).build();

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMetricsRW.class);

    /**
     * Checks if Locator is recently inserted
     *
     * @param loc
     * @return
     */
    // I don't like making this public, but currently DiscoveryWriter
    // calls this
    public synchronized boolean isLocatorCurrent(Locator loc) {
        return insertedLocators.getIfPresent(loc.toString()) != null;
    }

    /**
     * Marks the Locator as recently inserted
     * @param loc
     */
    protected synchronized void setLocatorCurrent(Locator loc) {
        insertedLocators.put(loc.toString(), Boolean.TRUE);
    }

    /**
     * Convert a collection of {@link com.rackspacecloud.blueflood.types.IMetric}
     * to a {@link com.google.common.collect.Multimap}
     *
     * @param metrics
     * @return
     */
    protected Multimap<Locator, IMetric> asMultimap(Collection<IMetric> metrics) {
        Multimap<Locator, IMetric> map = LinkedListMultimap.create();
        for (IMetric metric: metrics)
            map.put(metric.getLocator(), metric);
        return map;
    }

    /**
     * Gets the TTL for a particular locator, rollupType and granularity.
     *
     * @param locator
     * @param rollupType
     * @param granularity
     * @return
     */
    protected int getTtl(Locator locator, RollupType rollupType, Granularity granularity) {
        return (int) TTL_PROVIDER.getTTL(locator.getTenantId(),
                    granularity,
                    rollupType).get().toSeconds();
    }

    /**
     * Converts the map of timestamp -> {@link Rollup} to
     * {@link Points} object
     *
     * @param timestampToRollupMap  a map of timestamp to rollup
     * @return
     */
    protected <T extends Object> Points<T> convertToPoints(final Map<Long, T> timestampToRollupMap) {
        Points points =  new Points();
        for (Map.Entry<Long, T> value : timestampToRollupMap.entrySet() ) {

            points.add( createPoint( value.getKey(), value.getValue() ) );
        }
        return points;
    }

    protected Points.Point createPoint( Long timestamp, Object value ) {
        if( value instanceof Rollup )
            return new Points.Point( timestamp, value);
        else
            return new Points.Point( timestamp, new SimpleNumber( value ) );
    }
}
