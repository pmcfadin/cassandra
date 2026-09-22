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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.SinglePartitionReadCommand;
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
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.txn.TransactionCondition.Kind;
import org.apache.cassandra.utils.ByteBufferUtil;

/** Backend-neutral, bounded compiler for the test-only scalar integer profile. */
public final class ScalarTransactionPlan
{
    public static final int MAX_OPERATIONS = 16;

    public final List<Read> reads;
    public final List<Condition> conditions;
    public final List<Write> writes;
    public final String returnShape;

    private ScalarTransactionPlan(List<Read> reads, List<Condition> conditions, List<Write> writes, String returnShape)
    {
        this.reads = immutable(reads);
        this.conditions = immutable(conditions);
        this.writes = immutable(writes);
        this.returnShape = returnShape;
    }

    public static ScalarTransactionPlan compile(TransactionPlan plan, TransactionExecutionContext context)
    {
        if (plan == null || context == null)
            throw new InvalidRequestException("scalar-int requires a plan and context");
        if (context.consistency() != ConsistencyLevel.ONE || context.serialConsistency() != ConsistencyLevel.SERIAL)
            throw new InvalidRequestException("scalar-int requires consistency ONE and serial consistency SERIAL");
        if (plan.tables().size() != 1)
            throw new InvalidRequestException("scalar-int supports exactly one table");
        TableMetadata table = plan.tables().values().iterator().next();
        validateTable(table);
        if (plan.reads().size() > MAX_OPERATIONS || plan.conditions().size() > MAX_OPERATIONS || plan.writes().size() > MAX_OPERATIONS)
            throw new InvalidRequestException("scalar-int request exceeds operation limit");

        Map<Integer, Read> readsBySlot = new LinkedHashMap<>();
        for (TransactionPlan.Read read : plan.reads())
        {
            SinglePartitionReadCommand command = read.command();
            if (!command.metadata().id.equals(table.id) || command.isRangeRequest() || !command.isLimitedToOnePartition() || !command.rowFilter().isEmpty())
                throw new InvalidRequestException("scalar-int supports only exact single-partition reads");
            ByteBuffer key = command.partitionKey().getKey();
            if (key == null || key.remaining() != Integer.BYTES || readsBySlot.put(read.slot(), new Read(read.slot(), key.getInt(key.position()))) != null)
                throw new InvalidRequestException("scalar-int has invalid or duplicate read slot");
        }

        List<Condition> conditions = new ArrayList<>();
        for (TransactionCondition condition : plan.conditions())
        {
            TransactionReference reference = condition.reference();
            validateReference(reference, table, readsBySlot.keySet(), "condition");
            Integer literal = condition.value() == null ? null : intValue(condition.value());
            conditions.add(new Condition(reference.slot(), reference.column() == null, condition.kind(), literal));
        }

        for (Integer slot : plan.returning().slots())
            if (!readsBySlot.containsKey(slot))
                throw new InvalidRequestException("scalar-int return selection references an unknown read slot");
        for (TransactionReference reference : plan.returning().references())
        {
            validateReference(reference, table, readsBySlot.keySet(), "return");
            if (reference.column() == null)
                throw new InvalidRequestException("scalar-int does not support whole-row return references");
        }

        List<Write> writes = new ArrayList<>();
        Set<Integer> writeKeys = new HashSet<>();
        for (TransactionPlan.Write write : plan.writes())
        {
            validateWrite(write, table, readsBySlot.keySet());
            ByteBuffer key = write.baseUpdate().partitionKey().getKey();
            if (key.remaining() != Integer.BYTES)
                throw new InvalidRequestException("scalar-int requires integer keys");
            int intKey = key.getInt(key.position());
            Integer baseValue = baseValue(write.baseUpdate(), table);
            if (baseValue != null)
                addWrite(writes, writeKeys, new Write(intKey, baseValue, null));
            for (TransactionOperation operation : write.regularOperations())
            {
                if (operation.kind() != TransactionOperation.Kind.ConstantSetter || !operation.receiver().equals(table.regularColumns().iterator().next()) || operation.keyOrIndex() != null || operation.field() != null)
                    throw new InvalidRequestException("scalar-int supports only value-column setters");
                if (operation.value().kind() == TransactionValue.Kind.LITERAL)
                    addWrite(writes, writeKeys, new Write(intKey, intValue(operation.value().literal()), null));
                else
                    addWrite(writes, writeKeys, new Write(intKey, null, operation.value().reference().slot()));
            }
            if (baseValue == null && write.regularOperations().isEmpty())
                throw new InvalidRequestException("scalar-int write has no scalar value");
        }
        return new ScalarTransactionPlan(new ArrayList<>(readsBySlot.values()), conditions, writes, returnShape(plan));
    }

    public static TransactionOutcome materialize(TransactionPlan plan, long atMicros, Map<Integer, Integer> reads)
    {
        if (plan == null || reads == null || atMicros < 0)
            throw new InvalidRequestException("scalar-int materialization requires a plan, timestamp and reads");
        TableMetadata table = plan.tables().values().iterator().next();
        Map<Integer, FilteredPartition> partitions = new LinkedHashMap<>();
        for (TransactionPlan.Read read : plan.reads())
        {
            if (!reads.containsKey(read.slot()))
                continue;
            Integer value = reads.get(read.slot());
            partitions.put(read.slot(), partition(table, read.command().partitionKey(), value, atMicros));
        }
        return new TransactionOutcome(atMicros, partitions);
    }

    private static void addWrite(List<Write> writes, Set<Integer> keys, Write write)
    {
        if (!keys.add(write.key))
            throw new InvalidRequestException("scalar-int has duplicate write key");
        writes.add(write);
    }

    private static FilteredPartition partition(TableMetadata table, org.apache.cassandra.db.DecoratedKey key, Integer value, long atMicros)
    {
        if (value == null)
            return FilteredPartition.create(EmptyIterators.row(table, key, false));
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(table, key);
        builder.timestamp(atMicros);
        builder.row().add(table.regularColumns().iterator().next().name.toString(), value);
        try (UnfilteredRowIterator iterator = builder.build().unfilteredIterator())
        {
            return FilteredPartition.create(UnfilteredRowIterators.filter(iterator, atMicros / 1_000_000L));
        }
    }

    private static void validateTable(TableMetadata table)
    {
        if (table.partitionKeyColumns().size() != 1 || table.clusteringColumns().size() != 0
            || table.staticColumns().size() != 0 || table.regularColumns().size() != 1
            || table.partitionKeyColumns().get(0).type != Int32Type.instance
            || table.regularColumns().iterator().next().type != Int32Type.instance || table.isCounter())
            throw new InvalidRequestException("scalar-int requires one int partition key and one int value column");
    }

    private static void validateReference(TransactionReference reference, TableMetadata table, Set<Integer> slots, String where)
    {
        ColumnMetadata valueColumn = table.regularColumns().iterator().next();
        if (reference == null || !slots.contains(reference.slot()) || reference.path() != null
            || (reference.table() != null && !reference.table().id.equals(table.id))
            || (reference.column() != null && !reference.column().equals(valueColumn)))
            throw new InvalidRequestException("scalar-int " + where + " has an unsupported reference");
    }

    private static void validateWrite(TransactionPlan.Write write, TableMetadata table, Set<Integer> slots)
    {
        if (!write.baseUpdate().metadata().id.equals(table.id) || !write.clusterings().stream().allMatch(Clustering.EMPTY::equals) || !write.staticOperations().isEmpty())
            throw new InvalidRequestException("scalar-int supports only unclustered value writes");
        validateBaseUpdate(write.baseUpdate(), table);
        for (TransactionOperation operation : write.regularOperations())
        {
            if (!operation.table().equals(table) || operation.value() == null)
                throw new InvalidRequestException("scalar-int has an invalid write operation");
            if (operation.value().kind() == TransactionValue.Kind.REFERENCE)
            {
                if (operation.value().reference().column() == null)
                    throw new InvalidRequestException("scalar-int does not support whole-row write references");
                validateReference(operation.value().reference(), table, slots, "write");
            }
        }
    }

    private static void validateBaseUpdate(PartitionUpdate update, TableMetadata table)
    {
        if (!update.partitionLevelDeletion().isLive())
            throw new InvalidRequestException("scalar-int does not support deletes");
        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            if (!iterator.staticRow().isEmpty())
                throw new InvalidRequestException("scalar-int does not support static rows");
            while (iterator.hasNext())
            {
                Unfiltered item = iterator.next();
                if (!(item instanceof Row))
                    throw new InvalidRequestException("scalar-int does not support range tombstones");
                Row row = (Row) item;
                if (!Clustering.EMPTY.equals(row.clustering()) || row.hasDeletion(0))
                    throw new InvalidRequestException("scalar-int does not support clustered or deleted rows");
                if (row.primaryKeyLivenessInfo().isExpiring())
                    throw new InvalidRequestException("scalar-int does not support expiring rows");
                Cell<?> cell = row.getCell(table.regularColumns().iterator().next());
                if (cell != null && (cell.isTombstone() || cell.ttl() != Cell.NO_TTL || cell.buffer().remaining() != Integer.BYTES))
                    throw new InvalidRequestException("scalar-int supports only nonnull int values");
            }
        }
    }

    private static Integer baseValue(PartitionUpdate update, TableMetadata table)
    {
        Integer value = null;
        for (Row row : update)
        {
            Cell<?> cell = row.getCell(table.regularColumns().iterator().next());
            if (cell != null && !cell.isTombstone())
                value = intValue(cell.buffer());
        }
        return value;
    }

    private static int intValue(ByteBuffer value)
    {
        if (value == null || value.remaining() != Integer.BYTES)
            throw new InvalidRequestException("scalar-int requires nonnull int values");
        return value.getInt(value.position());
    }

    private static String returnShape(TransactionPlan plan)
    {
        StringBuilder shape = new StringBuilder(plan.returning().kind().name()).append(':').append(plan.returning().slots());
        for (TransactionPlan.Read read : plan.reads())
        {
            shape.append("|read=").append(read.slot());
            for (ColumnMetadata column : read.command().columnFilter().queriedColumns())
                shape.append(':').append(ByteBufferUtil.bytesToHex(column.name.bytes));
            shape.append("|projection=");
            for (ColumnMetadata column : read.projectedColumns())
                shape.append(':').append(ByteBufferUtil.bytesToHex(column.name.bytes));
        }
        for (TransactionReference reference : plan.returning().references())
        {
            shape.append(':').append(reference.slot());
            if (reference.column() != null)
                shape.append(':').append(ByteBufferUtil.bytesToHex(reference.column().name.bytes));
            if (reference.path() != null)
                shape.append(':').append(reference.path());
        }
        try
        {
            return ByteBufferUtil.bytesToHex(ByteBuffer.wrap(MessageDigest.getInstance("SHA-256")
                                                                         .digest(shape.toString().getBytes(StandardCharsets.UTF_8))));
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError(e);
        }
    }

    private static <T> List<T> immutable(List<T> values)
    {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    public static final class Read
    {
        public final int slot;
        public final int key;

        public Read(int slot, int key)
        {
            if (slot < 0)
                throw new IllegalArgumentException("slot must be nonnegative");
            this.slot = slot;
            this.key = key;
        }
    }

    public static final class Condition
    {
        public final int slot;
        public final boolean rowReference;
        public final Kind kind;
        public final Integer value;

        public Condition(int slot, boolean rowReference, Kind kind, Integer value)
        {
            if (slot < 0)
                throw new IllegalArgumentException("slot must be nonnegative");
            if (kind == null || ((kind == Kind.IS_NULL || kind == Kind.IS_NOT_NULL) != (value == null)))
                throw new IllegalArgumentException("invalid scalar condition");
            if (rowReference && kind != Kind.IS_NULL && kind != Kind.IS_NOT_NULL)
                throw new IllegalArgumentException("whole-row references only support null predicates");
            this.slot = slot;
            this.rowReference = rowReference;
            this.kind = kind;
            this.value = value;
        }
    }

    public static final class Write
    {
        public final int key;
        public final Integer constant;
        public final Integer sourceSlot;

        public Write(int key, Integer constant, Integer sourceSlot)
        {
            if ((constant == null) == (sourceSlot == null) || (sourceSlot != null && sourceSlot < 0))
                throw new IllegalArgumentException("a scalar write must have exactly one source");
            this.key = key;
            this.constant = constant;
            this.sourceSlot = sourceSlot;
        }

        public static Write constant(int key, int value)
        {
            return new Write(key, value, null);
        }

        public static Write reference(int key, int slot)
        {
            return new Write(key, null, slot);
        }
    }
}
