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
import java.util.Objects;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/** A bound, deferred CQL update operation whose value may depend on a transaction read. */
public final class TransactionOperation
{
    public enum Kind
    {
        SetAdder,
        ConstantAdder,
        ListAppender,
        SetDiscarder,
        ListDiscarder,
        ListPrepender,
        MapPutter,
        ListSetter,
        SetSetter,
        MapSetter,
        UserTypeSetter,
        ConstantSetter,
        ConstantSubtracter,
        MapSetterByKey,
        ListSetterByIndex,
        UserTypeSetterByField,
        ListDiscarderByIndex,
        MapDiscarderByKey,
        SetElementDiscarder
    }

    private final Kind kind;
    private final ColumnMetadata receiver;
    private final TableMetadata table;
    private final ByteBuffer keyOrIndex;
    private final ByteBuffer field;
    private final TransactionValue value;

    public TransactionOperation(Kind kind,
                                ColumnMetadata receiver,
                                TableMetadata table,
                                ByteBuffer keyOrIndex,
                                ByteBuffer field,
                                TransactionValue value)
    {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.receiver = Objects.requireNonNull(receiver, "receiver");
        this.table = Objects.requireNonNull(table, "table");
        this.keyOrIndex = copy(keyOrIndex);
        this.field = copy(field);
        this.value = Objects.requireNonNull(value, "value");
    }

    public Kind kind()
    {
        return kind;
    }

    public ColumnMetadata receiver()
    {
        return receiver;
    }

    public TableMetadata table()
    {
        return table;
    }

    public ByteBuffer keyOrIndex()
    {
        return copy(keyOrIndex);
    }

    public ByteBuffer field()
    {
        return copy(field);
    }

    public TransactionValue value()
    {
        return value;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof TransactionOperation))
            return false;
        TransactionOperation that = (TransactionOperation) other;
        return kind == that.kind && receiver.equals(that.receiver) && table.equals(that.table)
               && buffersEqual(keyOrIndex, that.keyOrIndex) && buffersEqual(field, that.field)
               && value.equals(that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, receiver, table, bufferHash(keyOrIndex), bufferHash(field), value);
    }

    private static ByteBuffer copy(ByteBuffer value)
    {
        if (value == null || value == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return value;
        return ByteBufferUtil.clone(value);
    }

    private static boolean buffersEqual(ByteBuffer left, ByteBuffer right)
    {
        if (left == ByteBufferUtil.UNSET_BYTE_BUFFER || right == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return left == right;
        return Objects.equals(left, right);
    }

    private static int bufferHash(ByteBuffer value)
    {
        return value == ByteBufferUtil.UNSET_BYTE_BUFFER ? 0x4f1bbcdc : Objects.hashCode(value);
    }
}
