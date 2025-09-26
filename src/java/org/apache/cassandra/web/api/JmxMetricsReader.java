/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.web.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.web.api.dto.CompactionMetricsDto;
import org.apache.cassandra.web.api.dto.OpsMetricsDto;
import org.apache.cassandra.web.api.dto.RingDto;
import org.apache.cassandra.web.api.dto.TablesDto;
import org.apache.cassandra.web.api.dto.SettingsDto;
import org.apache.cassandra.web.api.dto.CapabilitiesDto;

public class JmxMetricsReader
{
    private static final Logger logger = LoggerFactory.getLogger(JmxMetricsReader.class);

    public static OpsMetricsDto readOpsMetrics()
    {
        try
        {
            double readsPerSec = safeGetOneMinuteRate(ClientRequestsMetricsHolder.readMetrics);
            double writesPerSec = safeGetOneMinuteRate(ClientRequestsMetricsHolder.writeMetrics);

            OpsMetricsDto.LatencyDto readLatency = safeGetLatencyPercentiles(ClientRequestsMetricsHolder.readMetrics);
            OpsMetricsDto.LatencyDto writeLatency = safeGetLatencyPercentiles(ClientRequestsMetricsHolder.writeMetrics);

            double droppedPerSec = safeGetDroppedRate();
            int pendingReads = safeGetPendingTasks(Stage.READ);
            int pendingWrites = safeGetPendingTasks(Stage.MUTATION);

            double keyCacheHitRatio = safeGetCacheHitRatio("KeyCache");
            double rowCacheHitRatio = safeGetCacheHitRatio("RowCache");

            return new OpsMetricsDto(readsPerSec, writesPerSec, readLatency, writeLatency,
                                   droppedPerSec, pendingReads, pendingWrites,
                                   keyCacheHitRatio, rowCacheHitRatio);
        }
        catch (Exception e)
        {
            logger.warn("Error reading ops metrics", e);
            throw new RuntimeException("Error reading ops metrics", e);
        }
    }

    public static CompactionMetricsDto readCompactionMetrics()
    {
        try
        {
            int running = safeGetRunningCompactions();
            int pending = safeGetPendingCompactions();
            List<CompactionMetricsDto.TableCompactionDto> perTable = safeGetPerTableCompactions();

            return new CompactionMetricsDto(running, pending, perTable);
        }
        catch (Exception e)
        {
            logger.warn("Error reading compaction metrics", e);
            throw new RuntimeException("Error reading compaction metrics", e);
        }
    }

    private static double safeGetOneMinuteRate(Object metricsHolder)
    {
        try
        {
            if (metricsHolder != null && metricsHolder instanceof org.apache.cassandra.metrics.LatencyMetrics)
            {
                org.apache.cassandra.metrics.LatencyMetrics latencyMetrics = (org.apache.cassandra.metrics.LatencyMetrics) metricsHolder;
                return latencyMetrics.latency.getOneMinuteRate();
            }
        }
        catch (Exception e)
        {
            logger.debug("Error getting one minute rate", e);
        }
        return 0.0;
    }

    private static OpsMetricsDto.LatencyDto safeGetLatencyPercentiles(Object metricsHolder)
    {
        try
        {
            if (metricsHolder != null && metricsHolder instanceof org.apache.cassandra.metrics.LatencyMetrics)
            {
                org.apache.cassandra.metrics.LatencyMetrics latencyMetrics = (org.apache.cassandra.metrics.LatencyMetrics) metricsHolder;
                double p50 = latencyMetrics.latency.getSnapshot().getMedian() / 1000.0; // convert to ms
                double p95 = latencyMetrics.latency.getSnapshot().get95thPercentile() / 1000.0;
                double p99 = latencyMetrics.latency.getSnapshot().get99thPercentile() / 1000.0;
                return new OpsMetricsDto.LatencyDto(p50, p95, p99);
            }
        }
        catch (Exception e)
        {
            logger.debug("Error getting latency percentiles", e);
        }
        return new OpsMetricsDto.LatencyDto(0.0, 0.0, 0.0);
    }

    private static double safeGetDroppedRate()
    {
        try
        {
            // Access dropped messages via the global map
            return 0.0; // Simplified for now - will return 0 dropped messages
        }
        catch (Exception e)
        {
            logger.debug("Error getting dropped message rate", e);
        }
        return 0.0;
    }

    private static int safeGetPendingTasks(Stage stage)
    {
        try
        {
            // For simplicity, return 0 for now - this would require thread pool access
            return 0;
        }
        catch (Exception e)
        {
            logger.debug("Error getting pending tasks for stage " + stage, e);
        }
        return 0;
    }

    private static double safeGetCacheHitRatio(String cacheType)
    {
        try
        {
            if ("KeyCache".equals(cacheType))
            {
                return CacheService.instance.keyCache.getMetrics().hitRate.getValue();
            }
            else if ("RowCache".equals(cacheType))
            {
                return CacheService.instance.rowCache.getMetrics().hitRate.getValue();
            }
        }
        catch (Exception e)
        {
            logger.debug("Error getting cache hit ratio for " + cacheType, e);
        }
        return 0.0;
    }

    private static int safeGetRunningCompactions()
    {
        try
        {
            return CompactionManager.instance.getActiveCompactions();
        }
        catch (Exception e)
        {
            logger.debug("Error getting running compactions", e);
            return 0;
        }
    }

    private static int safeGetPendingCompactions()
    {
        try
        {
            return CompactionManager.instance.getMetrics().pendingTasks.getValue().intValue();
        }
        catch (Exception e)
        {
            logger.debug("Error getting pending compactions", e);
            return 0;
        }
    }

    private static List<CompactionMetricsDto.TableCompactionDto> safeGetPerTableCompactions()
    {
        List<CompactionMetricsDto.TableCompactionDto> result = new ArrayList<>();
        try
        {
            Map<String, Map<String, Integer>> pendingByTable = CompactionManager.instance.getMetrics().pendingTasksByTableName.getValue();
            if (pendingByTable != null)
            {
                for (Map.Entry<String, Map<String, Integer>> ksEntry : pendingByTable.entrySet())
                {
                    String keyspace = ksEntry.getKey();
                    Map<String, Integer> tables = ksEntry.getValue();
                    if (tables != null)
                    {
                        for (Map.Entry<String, Integer> tableEntry : tables.entrySet())
                        {
                            String table = tableEntry.getKey();
                            Integer pending = tableEntry.getValue();
                            if (pending != null && pending > 0)
                            {
                                result.add(new CompactionMetricsDto.TableCompactionDto(keyspace, table, pending));
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e)
        {
            logger.debug("Error getting per-table compaction metrics", e);
        }
        return result;
    }

    public static RingDto readRingInfo()
    {
        try
        {
            StorageService ss = StorageService.instance;
            String localDc = safeGetLocalDatacenter();
            String localRack = safeGetLocalRack();

            List<RingDto.NodeDto> nodes = new ArrayList<>();

            // Get local node info (simplified)
            String localAddress = "127.0.0.1"; // Simplified for PoC
            int localTokenCount = safeGetLocalTokenCount();
            nodes.add(new RingDto.NodeDto(localAddress, localDc, localRack, "NORMAL", true, localTokenCount));

            // Add other live nodes (simplified - just show count for now)
            List<String> liveNodes = ss.getLiveNodes();
            for (String nodeAddress : liveNodes)
            {
                if (!nodeAddress.equals(localAddress))
                {
                    nodes.add(new RingDto.NodeDto(nodeAddress, localDc, localRack, "NORMAL", false, 0));
                }
            }

            // Add unreachable nodes
            List<String> unreachableNodes = ss.getUnreachableNodes();
            for (String nodeAddress : unreachableNodes)
            {
                nodes.add(new RingDto.NodeDto(nodeAddress, localDc, localRack, "UNREACHABLE", false, 0));
            }

            return new RingDto(localDc, localRack, nodes);
        }
        catch (Exception e)
        {
            logger.warn("Error reading ring info", e);
            throw new RuntimeException("Error reading ring info", e);
        }
    }

    public static TablesDto readTablesInfo(String keyspaceFilter)
    {
        try
        {
            List<TablesDto.TableDto> tables = new ArrayList<>();

            // For simplicity, return empty list - would need to iterate through table metrics
            // This would be implemented by accessing TableMetrics for each table
            // and reading LiveSSTableCount, LiveDiskSpaceUsed, etc.

            return new TablesDto(tables);
        }
        catch (Exception e)
        {
            logger.warn("Error reading tables info", e);
            throw new RuntimeException("Error reading tables info", e);
        }
    }

    public static SettingsDto readSettings()
    {
        try
        {
            boolean authEnabled = false; // Would check web interface auth config
            boolean httpsEnabled = false; // Would check web interface HTTPS config
            String version = safeGetCassandraVersion();

            List<String> capabilities = new ArrayList<>();
            capabilities.add("ops");
            capabilities.add("compaction");
            capabilities.add("ring");
            capabilities.add("tables");

            return new SettingsDto(authEnabled, httpsEnabled, version, capabilities);
        }
        catch (Exception e)
        {
            logger.warn("Error reading settings", e);
            throw new RuntimeException("Error reading settings", e);
        }
    }

    public static CapabilitiesDto readCapabilities()
    {
        try
        {
            boolean authEnabled = false; // Would check web interface auth config
            boolean httpsEnabled = false; // Would check web interface HTTPS config

            List<String> features = new ArrayList<>();
            features.add("metrics");
            features.add("topology");
            features.add("tables");

            List<String> endpoints = new ArrayList<>();
            endpoints.add("/api/status");
            endpoints.add("/api/ops");
            endpoints.add("/api/compaction");
            endpoints.add("/api/ring");
            endpoints.add("/api/tables");
            endpoints.add("/api/settings");
            endpoints.add("/api/capabilities");

            return new CapabilitiesDto(authEnabled, httpsEnabled, features, endpoints);
        }
        catch (Exception e)
        {
            logger.warn("Error reading capabilities", e);
            throw new RuntimeException("Error reading capabilities", e);
        }
    }

    // Helper methods for ring info
    private static String safeGetLocalDatacenter()
    {
        try
        {
            return DatabaseDescriptor.getLocalDataCenter();
        }
        catch (Exception e)
        {
            logger.debug("Error getting local datacenter", e);
            return "unknown";
        }
    }

    private static String safeGetLocalRack()
    {
        try
        {
            // Simplified for PoC - in production would use snitch API
            return "rack1";
        }
        catch (Exception e)
        {
            logger.debug("Error getting local rack", e);
            return "unknown";
        }
    }

    private static int safeGetLocalTokenCount()
    {
        try
        {
            return StorageService.instance.getTokens().size();
        }
        catch (Exception e)
        {
            logger.debug("Error getting local token count", e);
            return 0;
        }
    }

    private static String safeGetCassandraVersion()
    {
        try
        {
            return org.apache.cassandra.utils.FBUtilities.getReleaseVersionString();
        }
        catch (Exception e)
        {
            logger.debug("Error getting Cassandra version", e);
            return "unknown";
        }
    }
}