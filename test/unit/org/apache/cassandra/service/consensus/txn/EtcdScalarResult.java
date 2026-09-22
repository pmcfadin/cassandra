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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable receipt returned by the scalar backend. */
public final class EtcdScalarResult
{
    public static final int MAX_READS = EtcdScalarRequest.MAX_OPERATIONS;
    public static final int MAX_BYTES = 64 * 1024;
    private static final int MAGIC = 0x45545231; // ETR1

    public final boolean known;
    public final boolean conditionMet;
    public final long atMicros;
    public final long decisionPosition;
    public final Map<Integer, Row> reads;

    public EtcdScalarResult(boolean known, boolean conditionMet, long atMicros, long decisionPosition, Map<Integer, Row> reads)
    {
        if (atMicros < 0 || decisionPosition < 0)
            throw new IllegalArgumentException("result positions must be nonnegative");
        if (!known && (conditionMet || atMicros != 0 || decisionPosition != 0 || (reads != null && !reads.isEmpty())))
            throw new IllegalArgumentException("unknown result must be empty");
        if (known && decisionPosition == 0)
            throw new IllegalArgumentException("known result must have a decision position");
        this.known = known;
        this.conditionMet = conditionMet;
        this.atMicros = atMicros;
        this.decisionPosition = decisionPosition;
        if (reads == null || reads.size() > MAX_READS)
            throw new IllegalArgumentException("invalid read count");
        Map<Integer, Row> copy = new LinkedHashMap<>();
        for (Map.Entry<Integer, Row> entry : reads.entrySet())
        {
            if (entry.getKey() == null || entry.getKey() < 0 || entry.getValue() == null)
                throw new IllegalArgumentException("invalid read");
            if (copy.put(entry.getKey(), entry.getValue()) != null)
                throw new IllegalArgumentException("duplicate read slot");
        }
        this.reads = Collections.unmodifiableMap(copy);
    }

    public static EtcdScalarResult unknown()
    {
        return new EtcdScalarResult(false, false, 0, 0, Collections.emptyMap());
    }

    public byte[] encode()
    {
        try
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeBoolean(known);
            out.writeBoolean(conditionMet);
            out.writeLong(atMicros);
            out.writeLong(decisionPosition);
            out.writeByte(reads.size());
            for (Map.Entry<Integer, Row> entry : reads.entrySet())
            {
                out.writeInt(entry.getKey());
                out.writeBoolean(entry.getValue().present);
                if (entry.getValue().present)
                {
                    out.writeBoolean(entry.getValue().value != null);
                    if (entry.getValue().value != null)
                        out.writeInt(entry.getValue().value);
                }
            }
            out.flush();
            byte[] result = bytes.toByteArray();
            if (result.length > MAX_BYTES)
                throw new IllegalArgumentException("result exceeds 64 KiB");
            return result;
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public static EtcdScalarResult decode(byte[] encoded)
    {
        if (encoded == null || encoded.length > MAX_BYTES)
            throw new IllegalArgumentException("invalid result length");
        try
        {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded));
            if (in.readInt() != MAGIC)
                throw new IllegalArgumentException("unsupported result header");
            boolean known = readBoolean(in);
            boolean conditionMet = readBoolean(in);
            long atMicros = in.readLong();
            long position = in.readLong();
            int count = in.readUnsignedByte();
            if (count > MAX_READS)
                throw new IllegalArgumentException("too many reads");
            Map<Integer, Row> reads = new LinkedHashMap<>();
            for (int i = 0; i < count; i++)
            {
                int slot = in.readInt();
                if (slot < 0 || reads.containsKey(slot))
                    throw new IllegalArgumentException("invalid read slot");
                boolean present = readBoolean(in);
                Integer value = present && readBoolean(in) ? in.readInt() : null;
                reads.put(slot, new Row(present, value));
            }
            if (in.available() != 0)
                throw new IllegalArgumentException("trailing result bytes");
            EtcdScalarResult result = new EtcdScalarResult(known, conditionMet, atMicros, position, reads);
            if (!Arrays.equals(encoded, result.encode()))
                throw new IllegalArgumentException("noncanonical result encoding");
            return result;
        }
        catch (EOFException e)
        {
            throw new IllegalArgumentException("truncated result", e);
        }
        catch (IOException | RuntimeException e)
        {
            if (e instanceof IllegalArgumentException)
                throw (IllegalArgumentException) e;
            throw new IllegalArgumentException("malformed result", e);
        }
    }

    private static boolean readBoolean(DataInputStream in) throws IOException
    {
        int value = in.readUnsignedByte();
        if (value > 1)
            throw new IllegalArgumentException("noncanonical boolean");
        return value == 1;
    }

    public static final class Row
    {
        public final boolean present;
        public final Integer value;
        public Row(boolean present, Integer value)
        {
            if (present != (value != null))
                throw new IllegalArgumentException("scalar row presence must match its nonnull value");
            this.present = present;
            this.value = value;
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Row))
                return false;
            Row that = (Row) other;
            return present == that.present && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(present, value);
        }
    }
}
