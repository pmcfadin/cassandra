/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.schema.TableId;

public class EtcdScalarCodecTest
{
    private static final ExternalTransactionDomainBinding BINDING = new ExternalTransactionDomainBinding("00000000-0000-0000-0000-000000000001", "12");

    @Test
    public void requestRoundTripsAndBindsProjection()
    {
        EtcdScalarRequest request = request(Collections.singletonList(new EtcdScalarRequest.Read(4, 7)),
                                            Collections.singletonList(new EtcdScalarRequest.Condition(4, false, EtcdScalarRequest.Operator.EQ, 2)),
                                            Collections.singletonList(EtcdScalarRequest.Write.constant(8, 11)), "ROWS:[4]");
        byte[] encoded = request.encode();
        Assertions.assertThat(EtcdScalarRequest.decode(encoded).encode()).isEqualTo(encoded);
        Assertions.assertThat(EtcdScalarRequest.decode(encoded).contentHash()).isEqualTo(request.contentHash());
        EtcdScalarRequest differentProjection = new EtcdScalarRequest(request.domainId, request.tableId, request.requestId,
                                                                      request.generation, request.schemaEpoch, request.binding,
                                                                      request.reads, request.conditions, request.writes, "NONE");
        Assertions.assertThat(differentProjection.contentHash()).isNotEqualTo(request.contentHash());
    }

    @Test
    public void rejectsMalformedHeaderTrailingBytesAndNoncanonicalBoolean()
    {
        byte[] encoded = request(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), "all").encode();
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        Assertions.assertThatThrownBy(() -> EtcdScalarRequest.decode(trailing)).isInstanceOf(IllegalArgumentException.class);
        byte[] truncated = Arrays.copyOf(encoded, encoded.length - 1);
        Assertions.assertThatThrownBy(() -> EtcdScalarRequest.decode(truncated)).isInstanceOf(IllegalArgumentException.class);
        byte[] noncanonical = EtcdScalarResult.unknown().encode();
        noncanonical[4] = 2;
        Assertions.assertThatThrownBy(() -> EtcdScalarResult.decode(noncanonical)).isInstanceOf(IllegalArgumentException.class);
        encoded[0] = 0;
        Assertions.assertThatThrownBy(() -> EtcdScalarRequest.decode(encoded)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void rejectsDanglingSlotsAndDuplicateWrites()
    {
        Assertions.assertThatThrownBy(() -> request(Collections.emptyList(),
                                                    Collections.singletonList(new EtcdScalarRequest.Condition(3, false, EtcdScalarRequest.Operator.EQ, 1)),
                                                    Collections.emptyList(), "all"))
                  .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> request(Collections.singletonList(new EtcdScalarRequest.Read(1, 2)),
                                                    Collections.emptyList(),
                                                    Arrays.asList(EtcdScalarRequest.Write.constant(4, 1), EtcdScalarRequest.Write.constant(4, 2)),
                                                    "all"))
                  .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void resultRoundTripsUnknownAndReceipt()
    {
        EtcdScalarResult result = new EtcdScalarResult(true, false, 17, 4,
                                                        Collections.singletonMap(1, new EtcdScalarResult.Row(true, 7)));
        Assertions.assertThat(EtcdScalarResult.decode(result.encode()).encode()).isEqualTo(result.encode());
        Assertions.assertThat(EtcdScalarResult.decode(EtcdScalarResult.unknown().encode()).known).isFalse();
    }

    private static EtcdScalarRequest request(java.util.List<EtcdScalarRequest.Read> reads,
                                             java.util.List<EtcdScalarRequest.Condition> conditions,
                                             java.util.List<EtcdScalarRequest.Write> writes,
                                             String shape)
    {
        return new EtcdScalarRequest(UUID.randomUUID(), TableId.fromLong(7), UUID.randomUUID(), 3, 9,
                                     BINDING, reads, conditions, writes, shape);
    }
}
