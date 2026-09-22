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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Cassandra-owned transaction program passed from CQL planning to a provider.
 * Model collections are immutable; bound buffer accessors return defensive heap copies because
 * Cassandra's unsafe comparators do not support read-only heap buffers. Existing Cassandra
 * metadata, read commands, clusterings and partition updates are shared for read-only use.
 */
public final class TransactionPlan
{
    private final List<Read> reads;
    private final List<Write> writes;
    private final List<TransactionCondition> conditions;
    private final Map<TableId, TableMetadata> tables;
    private final ReturnSelection returning;
    private final long minEpoch;
    private final boolean noOp;

    public TransactionPlan(List<Read> reads,
                           List<Write> writes,
                           List<TransactionCondition> conditions,
                           Map<TableId, TableMetadata> tables,
                           ReturnSelection returning,
                           long minEpoch,
                           boolean noOp)
    {
        this.reads = immutableCopy(reads, "reads");
        this.writes = immutableCopy(writes, "writes");
        this.conditions = immutableCopy(conditions, "conditions");
        this.tables = immutableMapCopy(tables);
        this.returning = Objects.requireNonNull(returning, "returning");
        this.minEpoch = minEpoch;
        this.noOp = noOp;
    }

    public List<Read> reads()
    {
        return reads;
    }

    public List<Write> writes()
    {
        return writes;
    }

    public List<TransactionCondition> conditions()
    {
        return conditions;
    }

    public Map<TableId, TableMetadata> tables()
    {
        return tables;
    }

    public ReturnSelection returning()
    {
        return returning;
    }

    public long minEpoch()
    {
        return minEpoch;
    }

    public boolean isNoOp()
    {
        return noOp;
    }

    public static final class Read
    {
        private final int slot;
        private final SinglePartitionReadCommand command;
        private final List<ColumnMetadata> projectedColumns;

        public Read(int slot, SinglePartitionReadCommand command)
        {
            this(slot, command, Collections.emptyList());
        }

        public Read(int slot, SinglePartitionReadCommand command, List<ColumnMetadata> projectedColumns)
        {
            this.slot = slot;
            this.command = Objects.requireNonNull(command, "command");
            this.projectedColumns = List.copyOf(projectedColumns);
        }

        public int slot()
        {
            return slot;
        }

        public SinglePartitionReadCommand command()
        {
            return command;
        }

        /** Ordered CQL column projection, including primary-key columns absent from the storage filter. */
        public List<ColumnMetadata> projectedColumns()
        {
            return projectedColumns;
        }
    }

    /**
     * A bound write template. The base update uses placeholder timestamps; providers must finalize
     * timestamps during execution. Deferred operations consume pre-write read slots. The index
     * identifies the source modification and its corresponding AUTO_READ slot where one is needed.
     */
    public static final class Write
    {
        private final int index;
        private final PartitionUpdate baseUpdate;
        private final List<Clustering<?>> clusterings;
        private final List<TransactionOperation> regularOperations;
        private final List<TransactionOperation> staticOperations;

        public Write(int index,
                     PartitionUpdate baseUpdate,
                     List<Clustering<?>> clusterings,
                     List<TransactionOperation> regularOperations,
                     List<TransactionOperation> staticOperations)
        {
            this.index = index;
            this.baseUpdate = Objects.requireNonNull(baseUpdate, "baseUpdate");
            this.clusterings = immutableCopy(clusterings, "clusterings");
            this.regularOperations = immutableCopy(regularOperations, "regularOperations");
            this.staticOperations = immutableCopy(staticOperations, "staticOperations");
        }

        public int index()
        {
            return index;
        }

        public PartitionUpdate baseUpdate()
        {
            return baseUpdate;
        }

        public List<Clustering<?>> clusterings()
        {
            return clusterings;
        }

        public List<TransactionOperation> regularOperations()
        {
            return regularOperations;
        }

        public List<TransactionOperation> staticOperations()
        {
            return staticOperations;
        }
    }

    public static final class ReturnSelection
    {
        public enum Kind
        {
            NONE,
            ROWS,
            REFERENCES
        }

        private static final ReturnSelection NONE = new ReturnSelection(Kind.NONE, Collections.emptyList(), Collections.emptyList());

        private final Kind kind;
        private final List<Integer> slots;
        private final List<TransactionReference> references;

        private ReturnSelection(Kind kind, List<Integer> slots, List<TransactionReference> references)
        {
            this.kind = kind;
            this.slots = slots;
            this.references = references;
        }

        public static ReturnSelection none()
        {
            return NONE;
        }

        public static ReturnSelection rows(List<Integer> slots)
        {
            List<Integer> copy = immutableCopy(slots, "slots");
            for (Integer slot : copy)
            {
                if (slot == null)
                    throw new IllegalArgumentException("Return read slots must not contain null");
            }
            return new ReturnSelection(Kind.ROWS, copy, Collections.emptyList());
        }

        public static ReturnSelection references(List<TransactionReference> references)
        {
            return new ReturnSelection(Kind.REFERENCES,
                                       Collections.emptyList(),
                                       immutableCopy(references, "references"));
        }

        public Kind kind()
        {
            return kind;
        }

        public List<Integer> slots()
        {
            return slots;
        }

        public List<TransactionReference> references()
        {
            return references;
        }
    }

    private static <T> List<T> immutableCopy(List<T> values, String name)
    {
        Objects.requireNonNull(values, name);
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    private static <K, V> Map<K, V> immutableMapCopy(Map<K, V> values)
    {
        Objects.requireNonNull(values, "tables");
        return Collections.unmodifiableMap(new java.util.LinkedHashMap<>(values));
    }
}
