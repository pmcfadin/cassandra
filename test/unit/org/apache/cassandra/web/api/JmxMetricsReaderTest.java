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

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.web.api.dto.CompactionMetricsDto;
import org.apache.cassandra.web.api.dto.OpsMetricsDto;

public class JmxMetricsReaderTest
{
    @Test
    public void testReadOpsMetricsSafety()
    {
        // This test verifies that the metrics reader handles errors gracefully
        // and returns valid DTOs even when services are not initialized
        try
        {
            OpsMetricsDto metrics = JmxMetricsReader.readOpsMetrics();
            assertNotNull("OpsMetricsDto should not be null", metrics);
            assertNotNull("Read latency should not be null", metrics.readLatencyMs);
            assertNotNull("Write latency should not be null", metrics.writeLatencyMs);

            // When services fail to initialize, metrics should gracefully fall back to 0
            assertTrue("Read rate should be non-negative", metrics.readsPerSec >= 0);
            assertTrue("Write rate should be non-negative", metrics.writesPerSec >= 0);
            assertTrue("Dropped rate should be non-negative", metrics.droppedPerSec >= 0);
            assertTrue("Pending reads should be non-negative", metrics.pendingReads >= 0);
            assertTrue("Pending writes should be non-negative", metrics.pendingWrites >= 0);
            assertTrue("Key cache hit ratio should be non-negative", metrics.keyCacheHitRatio >= 0);
            assertTrue("Row cache hit ratio should be non-negative", metrics.rowCacheHitRatio >= 0);
        }
        catch (Exception e)
        {
            // The method should handle initialization failures gracefully and not propagate exceptions
            fail("JmxMetricsReader.readOpsMetrics() should handle initialization errors gracefully: " + e.getMessage());
        }
    }

    @Test
    public void testReadCompactionMetricsSafety()
    {
        // This test verifies that the compaction metrics reader handles errors gracefully
        try
        {
            CompactionMetricsDto metrics = JmxMetricsReader.readCompactionMetrics();
            assertNotNull("CompactionMetricsDto should not be null", metrics);
            assertNotNull("Per-table list should not be null", metrics.perTable);

            // When services fail to initialize, metrics should gracefully fall back to 0
            assertTrue("Running compactions should be non-negative", metrics.running >= 0);
            assertTrue("Pending compactions should be non-negative", metrics.pending >= 0);
        }
        catch (Exception e)
        {
            // The method should handle initialization failures gracefully and not propagate exceptions
            fail("JmxMetricsReader.readCompactionMetrics() should handle initialization errors gracefully: " + e.getMessage());
        }
    }
}