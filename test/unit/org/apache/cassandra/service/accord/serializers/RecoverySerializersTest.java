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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.messages.BeginRecovery;
import accord.primitives.Ballot;
import accord.primitives.FullKeyRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

public class RecoverySerializersTest
{
    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
    }

    /**
     * Bug: RecoverySerializers.request writes the executeAtOrTxnIdEpoch delta with
     * {@code out.writeUnsignedVInt(long)} (RecoverySerializers.java:74) but reads it back with
     * {@code in.readUnsignedVInt32()} (RecoverySerializers.java:88). {@code Timestamp.MAX_EPOCH}
     * explicitly allows epochs up to 2^48 - 1, so any BeginRecovery whose executeAtOrTxnIdEpoch is
     * more than {@code Integer.MAX_VALUE} (2^31 - 1) past {@code txnId.epoch()} is a value the data
     * model considers legal but the wire format cannot decode.
     *
     * Measured behavior (see B5-recoveryserializers-vint.md): readUnsignedVInt32's checkedCast
     * rejects rather than silently truncates in this case, so the concrete failure is an uncaught
     * VIntOutOfRangeException thrown out of deserialize() -- a guaranteed decode crash / recovery
     * livelock for the affected TxnId, not silent corruption of the epoch value.
     *
     * TRIGGER: a BeginRecovery whose executeAtOrTxnIdEpoch is more than Integer.MAX_VALUE past
     *          txnId.epoch(), so the encoded delta requires more than 32 bits.
     * HARNESS: direct call into RecoverySerializers.request.serialize/deserialize, matching how
     *          the accord messaging layer round-trips this message over the wire.
     * ORACLE:  RecoverySerializers.request.deserialize(...) must return a BeginRecovery whose
     *          executeAtOrTxnIdEpoch equals what was serialized, i.e. must NOT throw. Failure
     *          criterion: VIntCoding.VIntOutOfRangeException with message
     *          "<delta> is out of range for a 32-bit integer" is thrown instead.
     */
    @Test
    public void executeAtOrTxnIdEpochRoundTripsLargeDelta() throws IOException
    {
        Txn txn = AccordTestUtils.createTxn(0, 1);
        PartitionKey key = (PartitionKey) txn.keys().get(0);
        PartialTxn partialTxn = txn.slice(Ranges.of(TokenRange.fullRange(key.table(), getPartitioner())), true);

        TxnId txnId = AccordTestUtils.txnId(1, 100, 1);
        RoutingKey routingKey = key.toUnseekable();
        Route<?> scope = new FullKeyRoute(routingKey, new RoutingKey[]{ routingKey });

        // Delta exceeds Integer.MAX_VALUE (2^31 - 1) so the vint64 write and vint32 read disagree.
        // Still well within the legal epoch range: Timestamp.MAX_EPOCH == (1L << 48) - 1.
        long delta = (1L << 32) + 42L;
        long executeAtOrTxnIdEpoch = txnId.epoch() + delta;

        BeginRecovery recover = BeginRecovery.SerializationSupport.create(
            txnId, scope, txnId.epoch(), txnId.epoch(), partialTxn, Ballot.ZERO,
            null, executeAtOrTxnIdEpoch, 0);

        DataOutputBuffer out = new DataOutputBuffer();
        RecoverySerializers.request.serialize(recover, out, Version.LATEST);

        DataInputBuffer in = new DataInputBuffer(out.unsafeGetBufferAndFlip(), false);

        // EXPECTED (correct) behavior: this should return a BeginRecovery with
        // executeAtOrTxnIdEpoch == executeAtOrTxnIdEpoch (i.e. round-trips cleanly), just like every
        // other IVersionedSerializer in this package. On the current buggy code it instead throws.
        Assertions.assertThatCode(() -> RecoverySerializers.request.deserialize(in, Version.LATEST))
                  .describedAs("executeAtOrTxnIdEpoch (delta=%d beyond txnId.epoch()=%d) must round-trip " +
                               "through RecoverySerializers.request without throwing",
                               delta, txnId.epoch())
                  .doesNotThrowAnyException();
    }
}
