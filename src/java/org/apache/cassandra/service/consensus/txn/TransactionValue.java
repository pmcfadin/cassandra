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

/** A bound literal or a reference to a value read by a transaction. */
public final class TransactionValue
{
    public enum Kind
    {
        LITERAL,
        REFERENCE
    }

    private final Kind kind;
    private final ByteBuffer literal;
    private final TransactionReference reference;

    private TransactionValue(Kind kind, ByteBuffer literal, TransactionReference reference)
    {
        this.kind = kind;
        this.literal = copy(literal);
        this.reference = reference;
    }

    public static TransactionValue literal(ByteBuffer value)
    {
        return new TransactionValue(Kind.LITERAL, value, null);
    }

    public static TransactionValue reference(TransactionReference reference)
    {
        return new TransactionValue(Kind.REFERENCE, null, Objects.requireNonNull(reference, "reference"));
    }

    public Kind kind()
    {
        return kind;
    }

    public ByteBuffer literal()
    {
        return copy(literal);
    }

    public TransactionReference reference()
    {
        return reference;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof TransactionValue))
            return false;
        TransactionValue that = (TransactionValue) other;
        return kind == that.kind && buffersEqual(literal, that.literal) && Objects.equals(reference, that.reference);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, bufferHash(literal), reference);
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
