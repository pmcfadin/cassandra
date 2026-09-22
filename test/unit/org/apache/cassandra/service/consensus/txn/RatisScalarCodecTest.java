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
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.schema.TableId;

public class RatisScalarCodecTest
{
    private static final UUID DOMAIN = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final String GROUP = "00000000-0000-0000-0000-000000000002";

    @Test
    public void usesStandaloneTransactionShape()
    {
        RatisScalarRequest request = request();
        String json = new String(request.encode(), StandardCharsets.UTF_8);
        Assertions.assertThat(json).contains("\"op\":\"txn\"")
                  .contains("\"operator\":\"EQ\"")
                  .contains("\"value\":11")
                  .doesNotContain("constant")
                  .doesNotContain("rowReference");
        Assertions.assertThat(RatisScalarRequest.decode(request.encode()).encode()).isEqualTo(request.encode());
        Assertions.assertThat(RatisScalarRequest.decode(request.lookupBytes()).lookupBytes()).isEqualTo(request.lookupBytes());
    }

    @Test
    public void rejectsMalformedRequestsAndResults()
    {
        byte[] encoded = request().encode();
        Assertions.assertThatThrownBy(() -> RatisScalarRequest.decode((new String(encoded, StandardCharsets.UTF_8) + " trailing").getBytes(StandardCharsets.UTF_8)))
                  .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> RatisScalarRequest.decode(Arrays.copyOf(encoded, encoded.length - 1)))
                  .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> RatisScalarResult.decode("{\"known\":true,\"conditionMet\":false,\"atMicros\":1,\"decisionPosition\":1,\"reads\":{\"1\":2147483648}}".getBytes(StandardCharsets.UTF_8)))
                  .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void preservesUnknownAndAbsentRows()
    {
        RatisScalarResult unknown = RatisScalarResult.unknown();
        Assertions.assertThat(RatisScalarResult.decode(unknown.encode()).known).isFalse();
        RatisScalarResult result = new RatisScalarResult(true, true, 17, 4, Collections.singletonMap(1, null));
        Assertions.assertThat(RatisScalarResult.decode(result.encode()).reads).containsEntry(1, null);
    }

    private static RatisScalarRequest request()
    {
        return new RatisScalarRequest(DOMAIN, TableId.fromLong(7), UUID.fromString("00000000-0000-0000-0000-000000000003"), 3, 9,
                                      GROUP, 17,
                                      Collections.singletonList(new ScalarTransactionPlan.Read(4, 7)),
                                      Collections.singletonList(new ScalarTransactionPlan.Condition(4, false, TransactionCondition.Kind.EQUAL, 2)),
                                      Collections.singletonList(ScalarTransactionPlan.Write.constant(8, 11)), "ROWS:[4]");
    }
}
