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

/** Names for logical reads in a transaction program. */
public final class TransactionReadSlot
{
    private static final int INDEX_BITS = 26;
    private static final int INDEX_MASK = (1 << INDEX_BITS) - 1;
    private static final int KIND_SHIFT = INDEX_BITS;

    private TransactionReadSlot()
    {
    }

    public enum Kind
    {
        USER(0),
        RETURNING(1),
        AUTO_READ(2);

        private final int tag;

        Kind(int tag)
        {
            this.tag = tag;
        }
    }

    public static int id(Kind kind, int index)
    {
        if (kind == null)
            throw new IllegalArgumentException("Read slot kind must not be null");
        if (index < 0 || index > INDEX_MASK)
            throw new IllegalArgumentException("Read slot index is outside the 26-bit range: " + index);
        return (kind.tag << KIND_SHIFT) | index;
    }

    public static int id(Kind kind)
    {
        return id(kind, 0);
    }

    /** Returns the kind encoded in an id, retaining compatibility with Accord data names. */
    public static Kind kind(int id)
    {
        int tag = id >>> KIND_SHIFT;
        for (Kind kind : Kind.values())
        {
            if (kind.tag == tag)
                return kind;
        }
        throw new IllegalArgumentException("Unknown transaction read slot kind: " + tag);
    }

    /** Returns the 26-bit index encoded in an id. */
    public static int index(int id)
    {
        kind(id);
        return id & INDEX_MASK;
    }
}
