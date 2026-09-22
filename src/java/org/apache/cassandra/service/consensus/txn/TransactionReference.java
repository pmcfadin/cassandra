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
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.HeapCloner;

import static org.apache.cassandra.db.marshal.CollectionType.Kind.SET;

/**
 * A Cassandra transaction reference to a row or a value in one read slot.
 *
 * This class deliberately depends only on Cassandra's partition and schema
 * types.  It is shared by provider-neutral result rendering and Accord's
 * existing condition/update implementation.
 */
public final class TransactionReference
{
    private final int slot;
    @Nullable
    private final TableMetadata table;
    @Nullable
    private final ColumnMetadata column;
    @Nullable
    private final CellPath path;

    public TransactionReference(int slot,
                                @Nullable TableMetadata table,
                                @Nullable ColumnMetadata column,
                                @Nullable CellPath path)
    {
        if (column == null && (table != null || path != null))
            throw new IllegalArgumentException("A whole-row reference cannot specify table or path");
        if (column != null && table == null)
            throw new IllegalArgumentException("A column reference must specify a table");

        this.slot = slot;
        this.table = table;
        this.column = column;
        this.path = path == null ? null : path.clone(HeapCloner.instance);
    }

    public int slot()
    {
        return slot;
    }

    @Nullable
    public TableMetadata table()
    {
        return table;
    }

    @Nullable
    public ColumnMetadata column()
    {
        return column;
    }

    @Nullable
    public CellPath path()
    {
        return path == null ? null : path.clone(HeapCloner.instance);
    }

    @Nullable
    public Row row(@Nullable FilteredPartition partition)
    {
        if (partition == null)
            return null;
        if (column != null && column.isStatic())
            return partition.staticRow();

        assert partition.rowCount() <= 1 : "Multi-row references are not allowed";
        if (partition.rowCount() == 0)
            return null;
        return partition.getAtIdx(0);
    }

    @Nullable
    public ByteBuffer toByteBuffer(@Nullable FilteredPartition partition, AbstractType<?> receiver)
    {
        if (column == null)
            throw new IllegalStateException("A whole-row reference has no value");

        AbstractType<?> type = column.type;
        if (selectsPath())
        {
            if (type.isCollection())
            {
                CollectionType<?> collectionType = (CollectionType<?>) type;
                type = collectionType.kind == SET ? collectionType.nameComparator() : collectionType.valueComparator();
            }
            else if (type.isUDT())
            {
                type = getFieldSelectionType();
            }
        }

        AbstractType<?> receiverType = type.isFrozenCollection() ? receiver.freeze().unwrap() : receiver.unwrap();
        if (receiverType != type.unwrap())
            throw new IllegalArgumentException("Receiving type " + receiverType + " does not match " + type.unwrap());

        if (partition == null)
            return null;

        if (column.isPartitionKey())
            return getPartitionKey(partition);
        if (column.isClusteringColumn())
            return getClusteringKey(partition);

        ColumnData columnData = getColumnData(row(partition));
        if (columnData == null)
            return null;

        if (selectsComplex())
        {
            ComplexColumnData complex = (ComplexColumnData) columnData;
            if (type instanceof CollectionType)
                return ((CollectionType<?>) type).serializeForNativeProtocol(complex.iterator());
            if (type instanceof UserType)
                return ((UserType) type).serializeForNativeProtocol(complex.iterator());
            throw new UnsupportedOperationException("Unsupported complex type: " + type);
        }
        if (selectsFrozenCollectionElement())
            return getFrozenCollectionElement((Cell<?>) columnData);
        if (selectsFrozenUDTField())
            return getFrozenFieldValue((Cell<?>) columnData);

        Cell<?> cell = (Cell<?>) columnData;
        return selectsSetElement() ? cell.path().get(0) : cell.buffer();
    }

    private ByteBuffer getPartitionKey(FilteredPartition partition)
    {
        DecoratedKey key = partition.partitionKey();
        return partition.metadata().partitionKeyColumns().size() == 1
               ? key.getKey()
               : ((CompositeType) partition.metadata().partitionKeyType).split(key.getKey())[column.position()];
    }

    @Nullable
    private ByteBuffer getClusteringKey(FilteredPartition partition)
    {
        Row row = row(partition);
        return row == null ? null : row.clustering().bufferAt(column.position());
    }

    @Nullable
    private ColumnData getColumnData(@Nullable Row row)
    {
        if (row == null)
            return null;
        if (column.isComplex() && path == null)
            return row.getComplexColumnData(column);

        if (path != null && column.type.isMultiCell())
        {
            if (column.type.isCollection())
            {
                CollectionType<?> collectionType = (CollectionType<?>) column.type;
                if (collectionType.kind == CollectionType.Kind.LIST)
                    return row.getComplexColumnData(column).getCellByIndex(ByteBufferUtil.toInt(path.get(0)));
            }
            return row.getCell(column, path);
        }

        return row.getCell(column);
    }

    private ByteBuffer getFrozenCollectionElement(Cell<?> collection)
    {
        CollectionType<?> collectionType = (CollectionType<?>) column.type.unwrap();
        return collectionType.getSerializer().getSerializedValue(collection.buffer(), path.get(0), collectionType.nameComparator());
    }

    private ByteBuffer getFrozenFieldValue(Cell<?> udt)
    {
        UserType userType = (UserType) column.type.unwrap();
        int field = ByteBufferUtil.getUnsignedShort(path.get(0), 0);
        List<ByteBuffer> tuple = userType.unpack(udt.buffer());
        return tuple.size() > field ? tuple.get(field) : null;
    }

    private AbstractType<?> getFieldSelectionType()
    {
        UserType userType = (UserType) column.type;
        int field = ByteBufferUtil.getUnsignedShort(path.get(0), 0);
        return userType.fieldType(field);
    }

    private boolean selectsPath()
    {
        return path != null;
    }

    private boolean selectsComplex()
    {
        return column.isComplex() && path == null;
    }

    private boolean selectsSetElement()
    {
        return selectsPath() && column.type instanceof SetType;
    }

    private boolean selectsFrozenCollectionElement()
    {
        return selectsPath() && column.type.isFrozenCollection();
    }

    private boolean selectsFrozenUDTField()
    {
        return selectsPath() && column.type.isUDT() && !column.type.isMultiCell();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof TransactionReference))
            return false;
        TransactionReference that = (TransactionReference) o;
        return slot == that.slot && Objects.equals(table, that.table) && Objects.equals(column, that.column) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(slot, table, column, path);
    }

    @Override
    public String toString()
    {
        return "TransactionReference{" + "slot=" + slot + ", table=" + table + ", column=" + column + ", path=" + path + '}';
    }
}
