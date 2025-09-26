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

import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TablesDto
{
    @JsonProperty("tables")
    public final List<TableDto> tables;

    public TablesDto(List<TableDto> tables)
    {
        this.tables = tables;
    }

    public static class TableDto
    {
        @JsonProperty("keyspace")
        public final String keyspace;

        @JsonProperty("table")
        public final String table;

        @JsonProperty("liveSSTables")
        public final int liveSSTables;

        @JsonProperty("liveDiskBytes")
        public final long liveDiskBytes;

        @JsonProperty("totalDiskBytes")
        public final long totalDiskBytes;

        @JsonProperty("memtableBytes")
        public final long memtableBytes;

        @JsonProperty("bloomFalseRatio")
        public final double bloomFalseRatio;

        @JsonProperty("compressionRatio")
        public final double compressionRatio;

        public TableDto(String keyspace, String table, int liveSSTables, long liveDiskBytes,
                       long totalDiskBytes, long memtableBytes, double bloomFalseRatio, double compressionRatio)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.liveSSTables = liveSSTables;
            this.liveDiskBytes = liveDiskBytes;
            this.totalDiskBytes = totalDiskBytes;
            this.memtableBytes = memtableBytes;
            this.bloomFalseRatio = bloomFalseRatio;
            this.compressionRatio = compressionRatio;
        }
    }
}