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
package org.apache.cassandra.web.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OpsMetricsDto
{
    @JsonProperty("readsPerSec")
    public final double readsPerSec;

    @JsonProperty("writesPerSec")
    public final double writesPerSec;

    @JsonProperty("readLatencyMs")
    public final LatencyDto readLatencyMs;

    @JsonProperty("writeLatencyMs")
    public final LatencyDto writeLatencyMs;

    @JsonProperty("droppedPerSec")
    public final double droppedPerSec;

    @JsonProperty("pendingReads")
    public final int pendingReads;

    @JsonProperty("pendingWrites")
    public final int pendingWrites;

    @JsonProperty("keyCacheHitRatio")
    public final double keyCacheHitRatio;

    @JsonProperty("rowCacheHitRatio")
    public final double rowCacheHitRatio;

    public OpsMetricsDto(double readsPerSec, double writesPerSec, LatencyDto readLatencyMs, LatencyDto writeLatencyMs,
                        double droppedPerSec, int pendingReads, int pendingWrites,
                        double keyCacheHitRatio, double rowCacheHitRatio)
    {
        this.readsPerSec = readsPerSec;
        this.writesPerSec = writesPerSec;
        this.readLatencyMs = readLatencyMs;
        this.writeLatencyMs = writeLatencyMs;
        this.droppedPerSec = droppedPerSec;
        this.pendingReads = pendingReads;
        this.pendingWrites = pendingWrites;
        this.keyCacheHitRatio = keyCacheHitRatio;
        this.rowCacheHitRatio = rowCacheHitRatio;
    }

    public static class LatencyDto
    {
        @JsonProperty("p50")
        public final double p50;

        @JsonProperty("p95")
        public final double p95;

        @JsonProperty("p99")
        public final double p99;

        public LatencyDto(double p50, double p95, double p99)
        {
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
        }
    }
}