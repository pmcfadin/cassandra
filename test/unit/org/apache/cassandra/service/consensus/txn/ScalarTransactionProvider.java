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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;

/**
 * A deliberately bounded, test-only interpreter for the Cassandra transaction plan.
 *
 * This class has no storage, Accord, or CQL execution dependency.  It exists to make
 * the provider seam earn its shape: it reads the plan, evaluates its conditions and
 * references, and publishes a complete scalar snapshot atomically.
 */
public final class ScalarTransactionProvider implements TransactionProvider
{
    public static final String ID = "scalar-test";

    private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(EnumSet.of(Capability.STRICT_SERIALIZABLE,
                                                                                              Capability.READ,
                                                                                              Capability.WRITE,
                                                                                              Capability.CONDITIONAL));

    private final Map<ScalarKey, ScalarRow> rows = new HashMap<>();
    private final AtomicInteger invocations = new AtomicInteger();

    @Override
    public String id()
    {
        return ID;
    }

    @Override
    public Set<Capability> capabilities()
    {
        return CAPABILITIES;
    }

    @Override
    public synchronized TransactionOutcome execute(TransactionPlan plan, TransactionExecutionContext context)
    {
        invocations.incrementAndGet();
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(context, "context");

        if (plan.isNoOp())
            return TransactionOutcome.noOp();

        TableMetadata table = validatePlan(plan);
        long atMicros = Clock.Global.currentTimeMillis() * 1000L;

        Map<Integer, FilteredPartition> readResults = new LinkedHashMap<>();
        for (TransactionPlan.Read read : plan.reads())
        {
            DecoratedKey key = read.command().partitionKey();
            ScalarRow row = rows.get(new ScalarKey(table.id, key.getKey(), Clustering.EMPTY));
            readResults.put(read.slot(), partition(table, key, row, atMicros));
        }

        if (!conditionsApply(plan.conditions(), readResults))
            return new TransactionOutcome(atMicros, readResults);

        Map<ScalarKey, ScalarRow> next = copyRows(rows);
        for (TransactionPlan.Write write : plan.writes())
            applyWrite(write, table, readResults, next);

        rows.clear();
        rows.putAll(next);
        return new TransactionOutcome(atMicros, readResults);
    }

    /** Seed a row for focused unit tests without going through CQL or another provider. */
    public synchronized void seed(TableMetadata table, int key, Integer value)
    {
        validateTable(table);
        rows.put(new ScalarKey(table.id, ByteBufferUtil.bytes(key), Clustering.EMPTY), new ScalarRow(true, value));
    }

    public synchronized Integer value(TableMetadata table, int key)
    {
        validateTable(table);
        ScalarRow row = rows.get(new ScalarKey(table.id, ByteBufferUtil.bytes(key), Clustering.EMPTY));
        return row == null ? null : row.value;
    }

    public int invocationCount()
    {
        return invocations.get();
    }

    private static TableMetadata validatePlan(TransactionPlan plan)
    {
        if (plan.tables().size() != 1)
            reject("scalar-test supports exactly one table");

        TableMetadata table = plan.tables().values().iterator().next();
        validateTable(table);
        Set<Integer> slots = plan.reads().stream().map(TransactionPlan.Read::slot).collect(java.util.stream.Collectors.toSet());
        if (slots.size() != plan.reads().size())
            reject("scalar-test requires unique read slots");
        for (TransactionPlan.Read read : plan.reads())
        {
            if (!read.command().metadata().id.equals(table.id))
                reject("scalar-test does not support mixed-table reads");
            if (read.command().isRangeRequest() || !read.command().isLimitedToOnePartition())
                reject("scalar-test supports only fixed single-partition reads");
            if (!read.command().rowFilter().isEmpty())
                reject("scalar-test does not support row filters");
        }

        for (TransactionPlan.Write write : plan.writes())
        {
            if (!write.baseUpdate().metadata().id.equals(table.id))
                reject("scalar-test does not support mixed-table writes");
            for (Clustering<?> clustering : write.clusterings())
                if (!Clustering.EMPTY.equals(clustering))
                    reject("scalar-test does not support clustered writes");
            if (!write.staticOperations().isEmpty())
                reject("scalar-test does not support static writes");
            for (TransactionOperation operation : write.regularOperations())
            {
                if (operation.kind() != TransactionOperation.Kind.ConstantSetter || operation.keyOrIndex() != null || operation.field() != null)
                    reject("scalar-test supports only whole-column setters");
                if (!operation.table().id.equals(table.id))
                    reject("scalar-test does not support mixed-table write references");
                validateColumn(operation.receiver(), table, "write operation");
                validateValue(operation.value(), table, slots);
            }
            validateBaseUpdate(write.baseUpdate(), table);
        }

        for (TransactionCondition condition : plan.conditions())
        {
            validateReference(condition.reference(), table, slots, "condition");
            if (condition.reference().column() == null
                && condition.kind() != TransactionCondition.Kind.IS_NULL
                && condition.kind() != TransactionCondition.Kind.IS_NOT_NULL)
                reject("scalar-test only permits whole-row null checks");
            if (condition.kind() != TransactionCondition.Kind.IS_NULL && condition.kind() != TransactionCondition.Kind.IS_NOT_NULL)
            {
                if (condition.value() == null || condition.value() == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    reject("scalar-test does not support null or UNSET comparison literals");
                validateInt(condition.value(), "condition literal");
            }
        }

        TransactionPlan.ReturnSelection returning = plan.returning();
        for (Integer slot : returning.slots())
            if (!slots.contains(slot))
                reject("return selection references an unknown read slot");
        for (TransactionReference reference : returning.references())
        {
            if (reference.column() == null)
                reject("scalar-test does not support whole-row return references");
            validateReference(reference, table, slots, "return reference");
        }

        return table;
    }

    private static void validateTable(TableMetadata table)
    {
        if (table == null || table.partitionKeyColumns().size() != 1 || table.clusteringColumns().size() != 0
            || table.staticColumns().size() != 0 || table.regularColumns().size() != 1)
            reject("scalar-test requires one int partition key and one int value column");
        if (table.partitionKeyColumns().get(0).type != Int32Type.instance
            || table.regularColumns().iterator().next().type != Int32Type.instance)
            reject("scalar-test requires int key and value columns");
        if (table.isCounter())
            reject("scalar-test does not support counters");
    }

    private static void validateColumn(ColumnMetadata column, TableMetadata table, String where)
    {
        ColumnMetadata expected = column == null ? null : table.getColumn(column.name);
        if (expected == null || column != expected || column != table.regularColumns().iterator().next())
            reject(where + " must target the scalar value column");
    }

    private static void validateReference(TransactionReference reference, TableMetadata table, Set<Integer> slots, String where)
    {
        if (reference == null || !slots.contains(reference.slot()) || (reference.table() != null && !reference.table().id.equals(table.id)))
            reject(where + " references an unknown read slot or table");
        if (reference.column() != null)
            validateColumn(reference.column(), table, where);
        if (reference.path() != null)
            reject(where + " does not support collection or UDT paths");
    }

    private static void validateValue(TransactionValue value, TableMetadata table, Set<Integer> slots)
    {
        if (value == null)
            reject("write value is missing");
        if (value.kind() == TransactionValue.Kind.LITERAL)
        {
            if (value.literal() == null || value.literal() == ByteBufferUtil.UNSET_BYTE_BUFFER)
                reject("scalar-test does not support null or UNSET write literals");
            validateInt(value.literal(), "write literal");
        }
        else
        {
            if (value.reference() == null || value.reference().column() == null)
                reject("scalar-test does not support whole-row write references");
            validateReference(value.reference(), table, slots, "write reference");
        }
    }

    private static void validateInt(ByteBuffer value, String where)
    {
        if (value != null && value != ByteBufferUtil.UNSET_BYTE_BUFFER && value.remaining() != Integer.BYTES)
            reject(where + " must be a four-byte int");
    }

    private static void validateBaseUpdate(PartitionUpdate update, TableMetadata table)
    {
        if (!update.partitionLevelDeletion().isLive())
            reject("scalar-test does not support partition deletes");
        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            while (iterator.hasNext())
            {
                Unfiltered item = iterator.next();
                if (!(item instanceof Row))
                    reject("scalar-test does not support range tombstones");
                Row row = (Row) item;
                if (!Clustering.EMPTY.equals(row.clustering()) || row.hasDeletion(0))
                    reject("scalar-test does not support clustered or deleted rows");
                Cell<?> cell = row.getCell(table.regularColumns().iterator().next());
                if (cell != null && cell.isTombstone())
                    reject("scalar-test does not support cell deletes");
                if (cell != null && cell.ttl() != Cell.NO_TTL)
                    reject("scalar-test does not support expiring cells");
                if (cell != null)
                    validateInt(cell.buffer(), "write cell");
            }
        }
    }

    private static boolean conditionsApply(List<TransactionCondition> conditions, Map<Integer, FilteredPartition> reads)
    {
        for (TransactionCondition condition : conditions)
        {
            FilteredPartition partition = reads.get(condition.reference().slot());
            boolean exists = partition != null && !partition.isEmpty();
            if (condition.reference().column() == null)
            {
                if (condition.kind() == TransactionCondition.Kind.IS_NULL && exists)
                    return false;
                if (condition.kind() == TransactionCondition.Kind.IS_NOT_NULL && !exists)
                    return false;
                continue;
            }

            ByteBuffer actual = !exists ? null : condition.reference().toByteBuffer(partition, Int32Type.instance);
            switch (condition.kind())
            {
                case IS_NULL:
                    if (actual != null) return false;
                    break;
                case IS_NOT_NULL:
                    if (actual == null) return false;
                    break;
                case EQUAL:
                    if (!compare(actual, condition.value(), 0)) return false;
                    break;
                case NOT_EQUAL:
                    if (!compare(actual, condition.value(), 3)) return false;
                    break;
                case GREATER_THAN:
                    if (!compare(actual, condition.value(), 1)) return false;
                    break;
                case GREATER_THAN_OR_EQUAL:
                    if (!compare(actual, condition.value(), 2)) return false;
                    break;
                case LESS_THAN:
                    if (!compare(actual, condition.value(), -1)) return false;
                    break;
                case LESS_THAN_OR_EQUAL:
                    if (!compare(actual, condition.value(), -2)) return false;
                    break;
                default:
                    throw new AssertionError(condition.kind());
            }
        }
        return true;
    }

    private static boolean compare(ByteBuffer actual, ByteBuffer expected, int relation)
    {
        if (actual == null || expected == null || expected == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return false;
        int comparison = Int32Type.instance.compare(actual, expected);
        switch (relation)
        {
            case 0: return comparison == 0;
            case 1: return comparison > 0;
            case 2: return comparison >= 0;
            case -1: return comparison < 0;
            case -2: return comparison <= 0;
            case 3: return comparison != 0;
            default: throw new AssertionError(relation);
        }
    }

    private static void applyWrite(TransactionPlan.Write write,
                                   TableMetadata table,
                                   Map<Integer, FilteredPartition> reads,
                                   Map<ScalarKey, ScalarRow> next)
    {
        PartitionUpdate update = write.baseUpdate();
        DecoratedKey key = update.partitionKey();
        ScalarKey scalarKey = new ScalarKey(table.id, key.getKey(), Clustering.EMPTY);
        ScalarRow target = copyRow(next.get(scalarKey));
        target.exists = true;

        Row base = null;
        for (Row row : update)
            base = row;
        if (base != null)
        {
            Cell<?> cell = base.getCell(table.regularColumns().iterator().next());
            if (cell != null && !cell.isTombstone())
                target.value = intValue(cell.buffer());
        }

        for (TransactionOperation operation : write.regularOperations())
        {
            TransactionValue value = operation.value();
            if (value.kind() == TransactionValue.Kind.LITERAL)
                target.value = intValue(value.literal());
            else
            {
                FilteredPartition source = reads.get(value.reference().slot());
                ByteBuffer resolved = source == null ? null : value.reference().toByteBuffer(source, Int32Type.instance);
                if (resolved == null)
                    reject("scalar-test does not support null or absent write references");
                target.value = intValue(resolved);
            }
        }
        next.put(scalarKey, target);
    }

    private static Integer intValue(ByteBuffer value)
    {
        if (value == null)
            return null;
        if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
            reject("scalar-test encountered UNSET instead of a value");
        return value.getInt(value.position());
    }

    private static FilteredPartition partition(TableMetadata table, DecoratedKey key, ScalarRow row, long atMicros)
    {
        if (row == null || !row.exists)
            return FilteredPartition.create(EmptyIterators.row(table, key, false));

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(table, key);
        builder.timestamp(atMicros);
        Row.SimpleBuilder rowBuilder = builder.row();
        if (row != null && row.value != null)
            rowBuilder.add(table.regularColumns().iterator().next().name.toString(), row.value);
        PartitionUpdate update = builder.build();
        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            return FilteredPartition.create(UnfilteredRowIterators.filter(iterator, atMicros / 1_000_000L));
        }
    }

    private static Map<ScalarKey, ScalarRow> copyRows(Map<ScalarKey, ScalarRow> source)
    {
        Map<ScalarKey, ScalarRow> copy = new HashMap<>();
        source.forEach((key, row) -> copy.put(key, copyRow(row)));
        return copy;
    }

    private static ScalarRow copyRow(ScalarRow row)
    {
        return row == null ? new ScalarRow(false, null) : new ScalarRow(row.exists, row.value);
    }

    private static void reject(String message)
    {
        throw new InvalidRequestException(message);
    }

    private static final class ScalarRow
    {
        private boolean exists;
        private Integer value;

        private ScalarRow(boolean exists, Integer value)
        {
            this.exists = exists;
            this.value = value;
        }
    }

    private static final class ScalarKey
    {
        private final TableId table;
        private final ByteBuffer partitionKey;
        private final Clustering<?> clustering;

        private ScalarKey(TableId table, ByteBuffer partitionKey, Clustering<?> clustering)
        {
            this.table = table;
            this.partitionKey = partitionKey == null ? null : partitionKey.duplicate();
            this.clustering = clustering;
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof ScalarKey))
                return false;
            ScalarKey that = (ScalarKey) other;
            return table.equals(that.table) && Objects.equals(partitionKey, that.partitionKey) && clustering.equals(that.clustering);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, partitionKey, clustering);
        }
    }
}
