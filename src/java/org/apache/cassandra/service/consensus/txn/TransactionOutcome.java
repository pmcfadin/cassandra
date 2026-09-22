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

package org.apache.cassandra.service.consensus.txn;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.cassandra.db.partitions.FilteredPartition;

/**
 * Cassandra-owned data returned by a transaction provider.
 *
 * The partitions are materialized before this object is returned.  Providers
 * transfer ownership of those read-only in-memory values to the caller and
 * must not retain or mutate them after returning.
 */
public final class TransactionOutcome
{
    private static final TransactionOutcome NO_OP = new TransactionOutcome(0, Collections.emptyMap(), true);

    private final long atMicros;
    private final Map<Integer, FilteredPartition> partitions;
    private final boolean noOp;

    public TransactionOutcome(long atMicros, Map<Integer, FilteredPartition> partitions)
    {
        this(atMicros, partitions, false);
    }

    private TransactionOutcome(long atMicros, Map<Integer, FilteredPartition> partitions, boolean noOp)
    {
        this.atMicros = atMicros;
        Objects.requireNonNull(partitions, "partitions");
        Map<Integer, FilteredPartition> copy = new HashMap<>(partitions.size());
        partitions.forEach((slot, partition) -> copy.put(Objects.requireNonNull(slot, "slot"),
                                                         Objects.requireNonNull(partition, "partition")));
        this.partitions = Collections.unmodifiableMap(copy);
        this.noOp = noOp;
    }

    public static TransactionOutcome noOp()
    {
        return NO_OP;
    }

    public long atMicros()
    {
        return atMicros;
    }

    public Map<Integer, FilteredPartition> partitions()
    {
        return partitions;
    }

    @Nullable
    public FilteredPartition partition(int slot)
    {
        return partitions.get(slot);
    }

    public boolean isNoOp()
    {
        return noOp;
    }
}
