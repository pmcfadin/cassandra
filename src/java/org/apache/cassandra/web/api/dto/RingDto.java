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

public class RingDto
{
    @JsonProperty("localDatacenter")
    public final String localDatacenter;

    @JsonProperty("localRack")
    public final String localRack;

    @JsonProperty("nodes")
    public final List<NodeDto> nodes;

    public RingDto(String localDatacenter, String localRack, List<NodeDto> nodes)
    {
        this.localDatacenter = localDatacenter;
        this.localRack = localRack;
        this.nodes = nodes;
    }

    public static class NodeDto
    {
        @JsonProperty("address")
        public final String address;

        @JsonProperty("datacenter")
        public final String datacenter;

        @JsonProperty("rack")
        public final String rack;

        @JsonProperty("state")
        public final String state;

        @JsonProperty("isLocal")
        public final boolean isLocal;

        @JsonProperty("tokenCount")
        public final int tokenCount;

        public NodeDto(String address, String datacenter, String rack, String state, boolean isLocal, int tokenCount)
        {
            this.address = address;
            this.datacenter = datacenter;
            this.rack = rack;
            this.state = state;
            this.isLocal = isLocal;
            this.tokenCount = tokenCount;
        }
    }
}