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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.schema.TableId;

public class RatisClientEnvelopeTest
{
    @Test
    public void roundTripsIdentityAndDigest()
    {
        RatisClientEnvelope envelope = RatisClientEnvelope.create("BEGIN TRANSACTION;", request());
        RatisClientEnvelope decoded = RatisClientEnvelope.decode(envelope.encode());
        Assertions.assertThat(decoded.cql).isEqualTo(envelope.cql);
        Assertions.assertThat(decoded.request.encode()).isEqualTo(envelope.request.encode());
        Assertions.assertThat(decoded.digest).isEqualTo(envelope.digest);
        Assertions.assertThat(envelope.digest).matches("[0-9a-f]{64}");
    }

    @Test
    public void rejectsTamperingAndNoncanonicalRequest()
    {
        byte[] encoded = RatisClientEnvelope.create("SELECT 1", request()).encode();
        String text = new String(encoded, StandardCharsets.UTF_8);
        Assertions.assertThatThrownBy(() -> RatisClientEnvelope.decode(text.replace("SELECT 1", "SELECT 2").getBytes(StandardCharsets.UTF_8)))
                  .isInstanceOf(IllegalArgumentException.class);
        String request = java.util.Base64.getEncoder().encodeToString(request().lookupBytes());
        String lookup = text.replaceFirst("\\\"request\\\":\\\"[^\\\"]+\\\"", "\\\"request\\\":\\\"" + request + "\\\"");
        Assertions.assertThatThrownBy(() -> RatisClientEnvelope.decode(lookup.getBytes(StandardCharsets.UTF_8)))
                  .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void rejectsMalformedOversizedDuplicateAndTrailingInput()
    {
        RatisClientEnvelope envelope = RatisClientEnvelope.create("SELECT 1", request());
        String text = new String(envelope.encode(), StandardCharsets.UTF_8);
        Assertions.assertThatThrownBy(() -> RatisClientEnvelope.decode((text + " trailing").getBytes(StandardCharsets.UTF_8)))
                  .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> RatisClientEnvelope.decode((text.substring(0, text.length() - 1) + ",\"v\":1}").getBytes(StandardCharsets.UTF_8)))
                  .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> RatisClientEnvelope.create("x".repeat(RatisClientEnvelope.MAX_CQL_BYTES + 1), request()))
                  .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> RatisClientEnvelope.decode(new byte[RatisClientEnvelope.MAX_BYTES + 1]))
                  .isInstanceOf(IllegalArgumentException.class);
    }

    private static RatisScalarRequest request()
    {
        return new RatisScalarRequest(UUID.fromString("00000000-0000-0000-0000-000000000001"), TableId.fromLong(7),
                                      UUID.fromString("00000000-0000-0000-0000-000000000003"), 3, 9,
                                      "00000000-0000-0000-0000-000000000002", 17,
                                      Collections.singletonList(new ScalarTransactionPlan.Read(4, 7)),
                                      Collections.singletonList(new ScalarTransactionPlan.Condition(4, false, TransactionCondition.Kind.EQUAL, 2)),
                                      Collections.singletonList(ScalarTransactionPlan.Write.constant(8, 11)), "ROWS:[4]");
    }
}
