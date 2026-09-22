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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.schema.TableId;

/** Deterministic, bounded wire representation of one scalar etcd request. */
public final class EtcdScalarRequest
{
    public static final String PROVIDER = "etcd-scalar";
    public static final String PROFILE = "scalar-int";
    public static final int VERSION = 1;
    public static final int MAX_OPERATIONS = 16;
    public static final int MAX_BYTES = 64 * 1024;
    private static final int MAGIC = 0x45545331; // ETS1

    public final UUID domainId;
    public final TableId tableId;
    public final UUID requestId;
    public final long generation;
    public final long schemaEpoch;
    public final ExternalTransactionDomainBinding binding;
    public final List<Read> reads;
    public final List<Condition> conditions;
    public final List<Write> writes;
    public final String returnShape;

    public EtcdScalarRequest(UUID domainId,
                             TableId tableId,
                             UUID requestId,
                             long generation,
                             long schemaEpoch,
                             ExternalTransactionDomainBinding binding,
                             List<Read> reads,
                             List<Condition> conditions,
                             List<Write> writes)
    {
        this(domainId, tableId, requestId, generation, schemaEpoch, binding, reads, conditions, writes, "all");
    }

    public EtcdScalarRequest(UUID domainId,
                             TableId tableId,
                             UUID requestId,
                             long generation,
                             long schemaEpoch,
                             ExternalTransactionDomainBinding binding,
                             List<Read> reads,
                             List<Condition> conditions,
                             List<Write> writes,
                             String returnShape)
    {
        this.domainId = require(domainId, "domainId");
        this.tableId = require(tableId, "tableId");
        this.requestId = require(requestId, "requestId");
        if (generation <= 0 || schemaEpoch < 0)
            throw new IllegalArgumentException("generation must be positive and schemaEpoch nonnegative");
        this.generation = generation;
        this.schemaEpoch = schemaEpoch;
        this.binding = require(binding, "binding");
        this.reads = immutable(reads, "reads");
        this.conditions = immutable(conditions, "conditions");
        this.writes = immutable(writes, "writes");
        if (returnShape == null || returnShape.length() > 256)
            throw new IllegalArgumentException("returnShape is missing or too long");
        this.returnShape = returnShape;
        validateShape();
    }

    public byte[] encode()
    {
        try
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            writeString(out, PROVIDER);
            writeString(out, PROFILE);
            out.writeByte(VERSION);
            out.writeByte(VERSION);
            out.writeByte(VERSION);
            writeUuid(out, domainId);
            writeUuid(out, tableId.asUUID());
            writeUuid(out, requestId);
            out.writeLong(generation);
            out.writeLong(schemaEpoch);
            writeString(out, binding.groupId());
            writeString(out, binding.clusterId());
            writeString(out, returnShape);
            out.writeByte(reads.size());
            for (Read read : reads)
            {
                out.writeInt(read.slot);
                out.writeInt(read.key);
            }
            out.writeByte(conditions.size());
            for (Condition condition : conditions)
            {
                out.writeInt(condition.slot);
                out.writeBoolean(condition.rowReference);
                out.writeByte(condition.operator.ordinal());
                out.writeBoolean(condition.value != null);
                if (condition.value != null)
                    out.writeInt(condition.value);
            }
            out.writeByte(writes.size());
            for (Write write : writes)
            {
                out.writeInt(write.key);
                out.writeBoolean(write.constant != null);
                if (write.constant != null)
                    out.writeInt(write.constant);
                else
                    out.writeInt(write.sourceSlot);
            }
            out.flush();
            byte[] result = bytes.toByteArray();
            if (result.length > MAX_BYTES)
                throw new IllegalArgumentException("request exceeds 64 KiB");
            return result;
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public byte[] contentHash()
    {
        try
        {
            return MessageDigest.getInstance("SHA-256").digest(encode());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError(e);
        }
    }

    public static EtcdScalarRequest decode(byte[] encoded)
    {
        if (encoded == null || encoded.length > MAX_BYTES)
            throw new IllegalArgumentException("invalid request length");
        try
        {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded));
            if (in.readInt() != MAGIC || in.readUnsignedByte() != VERSION)
                throw new IllegalArgumentException("unsupported request header");
            if (!PROVIDER.equals(readString(in)) || !PROFILE.equals(readString(in)))
                throw new IllegalArgumentException("unsupported provider/profile");
            if (in.readUnsignedByte() != VERSION || in.readUnsignedByte() != VERSION || in.readUnsignedByte() != VERSION)
                throw new IllegalArgumentException("unsupported protocol/profile/storage version");
            UUID domain = readUuid(in);
            UUID table = readUuid(in);
            UUID request = readUuid(in);
            long generation = in.readLong();
            long epoch = in.readLong();
            String group = readString(in);
            String cluster = readString(in);
            String returnShape = readString(in);
            int readCount = count(in.readUnsignedByte());
            int conditionCount;
            List<Read> reads = new ArrayList<>(readCount);
            for (int i = 0; i < readCount; i++)
                reads.add(new Read(in.readInt(), in.readInt()));
            conditionCount = count(in.readUnsignedByte());
            List<Condition> conditions = new ArrayList<>(conditionCount);
            for (int i = 0; i < conditionCount; i++)
            {
                int slot = in.readInt();
                boolean row = readBoolean(in);
                int op = in.readUnsignedByte();
                if (op >= Operator.values().length)
                    throw new IllegalArgumentException("invalid operator");
                Integer value = readBoolean(in) ? in.readInt() : null;
                conditions.add(new Condition(slot, row, Operator.values()[op], value));
            }
            int writeCount = count(in.readUnsignedByte());
            List<Write> writes = new ArrayList<>(writeCount);
            for (int i = 0; i < writeCount; i++)
            {
                int key = in.readInt();
                writes.add(readBoolean(in) ? Write.constant(key, in.readInt()) : Write.reference(key, in.readInt()));
            }
            if (in.available() != 0)
                throw new IllegalArgumentException("trailing request bytes");
            EtcdScalarRequest result = new EtcdScalarRequest(domain, TableId.fromUUID(table), request, generation, epoch,
                                                             new ExternalTransactionDomainBinding(group, cluster), reads, conditions, writes, returnShape);
            if (!Arrays.equals(encoded, result.encode()))
                throw new IllegalArgumentException("noncanonical request encoding");
            return result;
        }
        catch (EOFException e)
        {
            throw new IllegalArgumentException("truncated request", e);
        }
        catch (IOException | RuntimeException e)
        {
            if (e instanceof IllegalArgumentException)
                throw (IllegalArgumentException) e;
            throw new IllegalArgumentException("malformed request", e);
        }
    }

    private void validateShape()
    {
        if (reads.size() > MAX_OPERATIONS || conditions.size() > MAX_OPERATIONS || writes.size() > MAX_OPERATIONS)
            throw new IllegalArgumentException("too many scalar operations");
        Set<Integer> slots = new HashSet<>();
        for (Read read : reads)
            if (!slots.add(read.slot))
                throw new IllegalArgumentException("duplicate read slot");
        for (Condition condition : conditions)
            if (!slots.contains(condition.slot))
                throw new IllegalArgumentException("dangling condition slot");
        Set<Integer> keys = new HashSet<>();
        for (Write write : writes)
            if (!keys.add(write.key))
                throw new IllegalArgumentException("duplicate write key");
        for (Write write : writes)
            if (write.constant == null && !slots.contains(write.sourceSlot))
                throw new IllegalArgumentException("dangling write reference");
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

    public enum Operator
    {
        EQ, NEQ, LT, LTE, GT, GTE, IS_NULL, IS_NOT_NULL
    }

    public static final class Condition
    {
        public final int slot;
        public final boolean rowReference;
        public final Operator operator;
        public final Integer value;
        public Condition(int slot, boolean rowReference, Operator operator, Integer value)
        {
            if (slot < 0)
                throw new IllegalArgumentException("slot must be nonnegative");
            this.slot = slot;
            this.rowReference = rowReference;
            this.operator = require(operator, "operator");
            this.value = value;
            if ((operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL) != (value == null))
                throw new IllegalArgumentException("null operator value mismatch");
            if (rowReference && operator != Operator.IS_NULL && operator != Operator.IS_NOT_NULL)
                throw new IllegalArgumentException("whole-row references only support null predicates");
        }
    }

    public static final class Write
    {
        public final int key;
        public final Integer constant;
        public final int sourceSlot;

        private Write(int key, Integer constant, int sourceSlot)
        {
            this.key = key;
            this.constant = constant;
            this.sourceSlot = sourceSlot;
        }

        public static Write constant(int key, int value)
        {
            return new Write(key, value, -1);
        }

        public static Write reference(int key, int slot)
        {
            if (slot < 0)
                throw new IllegalArgumentException("slot must be nonnegative");
            return new Write(key, null, slot);
        }
    }

    private static int count(int value)
    {
        if (value > MAX_OPERATIONS)
            throw new IllegalArgumentException("too many operations");
        return value;
    }

    private static void writeUuid(DataOutputStream out, UUID value) throws IOException
    {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    private static UUID readUuid(DataInputStream in) throws IOException
    {
        return new UUID(in.readLong(), in.readLong());
    }

    private static void writeString(DataOutputStream out, String value) throws IOException
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 256)
            throw new IllegalArgumentException("string too long");
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException
    {
        int length = in.readUnsignedShort();
        if (length > 256)
            throw new IllegalArgumentException("string too long");
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static boolean readBoolean(DataInputStream in) throws IOException
    {
        int value = in.readUnsignedByte();
        if (value > 1)
            throw new IllegalArgumentException("noncanonical boolean");
        return value == 1;
    }

    private static <T> T require(T value, String name)
    {
        if (value == null)
            throw new NullPointerException(name);
        return value;
    }

    private static <T> List<T> immutable(List<T> value, String name)
    {
        require(value, name);
        List<T> copy = new ArrayList<>(value);
        for (T item : copy)
            require(item, name + " contains null");
        return Collections.unmodifiableList(copy);
    }
}
