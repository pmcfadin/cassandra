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

import org.apache.cassandra.utils.ByteBufferUtil;

/** One bound predicate over a value read by a transaction. Conditions are ANDed by the plan. */
public final class TransactionCondition
{
    public enum Kind
    {
        IS_NULL,
        IS_NOT_NULL,
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL
    }

    private final Kind kind;
    private final TransactionReference reference;
    private final ByteBuffer value;

    public TransactionCondition(Kind kind, TransactionReference reference, ByteBuffer value)
    {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.reference = Objects.requireNonNull(reference, "reference");
        this.value = copy(value);
    }

    public Kind kind()
    {
        return kind;
    }

    public TransactionReference reference()
    {
        return reference;
    }

    public ByteBuffer value()
    {
        return copy(value);
    }

    /** Reverses a comparison when binding a reference that appeared on the right-hand side. */
    public static Kind reverse(Kind kind)
    {
        Objects.requireNonNull(kind, "kind");
        switch (kind)
        {
            case GREATER_THAN: return Kind.LESS_THAN;
            case GREATER_THAN_OR_EQUAL: return Kind.LESS_THAN_OR_EQUAL;
            case LESS_THAN: return Kind.GREATER_THAN;
            case LESS_THAN_OR_EQUAL: return Kind.GREATER_THAN_OR_EQUAL;
            default: return kind;
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof TransactionCondition))
            return false;
        TransactionCondition that = (TransactionCondition) other;
        return kind == that.kind && reference.equals(that.reference) && buffersEqual(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, reference, bufferHash(value));
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
