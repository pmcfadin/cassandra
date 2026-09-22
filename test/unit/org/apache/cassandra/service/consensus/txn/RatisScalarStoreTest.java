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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.JsonUtils;

public class RatisScalarStoreTest
{
    private static final UUID DOMAIN = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID REQUEST = UUID.fromString("00000000-0000-0000-0000-000000000003");
    private static final String GROUP = "00000000-0000-0000-0000-000000000002";
    private HttpServer server;
    private AtomicReference<Response> response;
    private RatisScalarStore store;
    private RatisScalarRequest request;

    @Before
    public void setup() throws IOException
    {
        response = new AtomicReference<>(new Response(200, "{\"status\":\"ok\",\"known\":false}"));
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/command", this::serve);
        server.start();
        store = new RatisScalarStore(Collections.singletonList("http://127.0.0.1:" + server.getAddress().getPort()), 1000);
        request = new RatisScalarRequest(DOMAIN, TableId.fromLong(7), REQUEST, 3, 9, GROUP, 17,
                                         Collections.singletonList(new ScalarTransactionPlan.Read(4, 7)),
                                         Collections.emptyList(), Collections.emptyList(), "ROWS:[4]");
    }

    @After
    public void cleanup()
    {
        store.close();
        server.stop(0);
    }

    @Test
    public void missingLookupIsUnknown()
    {
        Assertions.assertThat(store.lookup(request).known).isFalse();
    }

    @Test
    public void knownReceiptMustMatchTimestampAndReadSlotsOnSubmitAndLookup()
    {
        response.set(new Response(200, "{\"status\":\"ok\",\"known\":true,\"conditionMet\":true,\"atMicros\":18,\"decisionPosition\":1,\"reads\":{\"4\":7}}"));
        Assertions.assertThatThrownBy(() -> store.submit(request)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        Assertions.assertThatThrownBy(() -> store.lookup(request)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        response.set(new Response(200, "{\"status\":\"ok\",\"known\":true,\"conditionMet\":true,\"atMicros\":17,\"decisionPosition\":1,\"reads\":{\"8\":7}}"));
        Assertions.assertThatThrownBy(() -> store.submit(request)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        Assertions.assertThatThrownBy(() -> store.lookup(request)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
    }

    @Test
    public void acceptsKnownReceiptWithShuffledResponseFields()
    {
        response.set(new Response(200, "{\"reads\":{\"4\":7},\"decisionPosition\":1,\"atMicros\":17,\"conditionMet\":true,\"known\":true,\"status\":\"ok\"}"));
        Assertions.assertThat(store.submit(request).reads).containsEntry(4, 7);
    }

    @Test
    public void transportAndMalformedResponsesAreUnknown()
    {
        response.set(new Response(503, "unavailable"));
        Assertions.assertThatThrownBy(() -> store.submit(request)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        response.set(new Response(200, "{bad"));
        Assertions.assertThatThrownBy(() -> store.submit(request)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        response.set(new Response(200, "{\"status\":\"ok\",\"known\":false} trailing"));
        Assertions.assertThatThrownBy(() -> store.submit(request)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        response.set(new Response(200, "{\"status\":\"ok\",\"status\":\"ok\",\"known\":false}"));
        Assertions.assertThatThrownBy(() -> store.submit(request)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
    }

    @Test
    public void explicitInitAndFenceRejectionsAreInvalidRequests()
    {
        response.set(new Response(200, "{\"status\":\"rejected\",\"reason\":\"ALREADY_INITIALIZED\"}"));
        Assertions.assertThatThrownBy(() -> store.prepareAndAttest(prepared(), 9, GROUP)).isInstanceOf(InvalidRequestException.class);
        Assertions.assertThatThrownBy(() -> store.fence(request, 4)).isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void attestationRequiresExactIdentityAndNumericFields()
    {
        ObjectNode valid = attestation();
        reply(valid);
        ExternalTransactionDomainBinding binding = store.attest(prepared(), 9, GROUP);
        Assertions.assertThat(binding.groupId()).isEqualTo(GROUP);
        Assertions.assertThat(binding.schemaEpoch()).isEqualTo(9);
        Assertions.assertThat(binding.readinessIndex()).isEqualTo(1);

        for (String field : new String[]{ "domain", "table", "group" })
        {
            reply(valid.deepCopy().put(field, UUID.randomUUID().toString()));
            Assertions.assertThatThrownBy(() -> store.attest(prepared(), 9, GROUP)).isInstanceOf(InvalidRequestException.class);
        }
        reply(valid.deepCopy().put("generation", "3"));
        Assertions.assertThatThrownBy(() -> store.attest(prepared(), 9, GROUP)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        reply(valid.deepCopy().put("schemaEpoch", 9.5));
        Assertions.assertThatThrownBy(() -> store.attest(prepared(), 9, GROUP)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        for (String field : new String[]{ "profileVersion", "protocolVersion", "storageVersion" })
        {
            reply(valid.deepCopy().put(field, 4294967297L));
            Assertions.assertThatThrownBy(() -> store.attest(prepared(), 9, GROUP)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        }
    }

    @Test
    public void attestationRejectsIneligibleReservationsBeforeTransport()
    {
        reply(attestation());
        TransactionDomainDescriptor active = prepared().activate(ExternalTransactionDomainBinding.ratis(GROUP, 9, 1));
        Assertions.assertThatThrownBy(() -> store.attest(active, 9, GROUP)).isInstanceOf(IllegalArgumentException.class);
        TransactionDomainDescriptor etcd = new TransactionDomainDescriptor(DOMAIN, TableId.fromLong(7), "etcd-scalar", "scalar-int", 1, 1, 1, 3,
                                                                           Collections.singleton(new org.apache.cassandra.tcm.membership.NodeId(1)));
        Assertions.assertThatThrownBy(() -> store.attest(etcd, 9, GROUP)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void missingSubmitAndMalformedFenceStayUnknown()
    {
        Assertions.assertThatThrownBy(() -> store.submit(request)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        response.set(new Response(200, "{\"status\":\"ok\"}"));
        Assertions.assertThatThrownBy(() -> store.fence(request, 4)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
        response.set(new Response(200, "{\"status\":\"unknown\"}"));
        Assertions.assertThatThrownBy(() -> store.fence(request, 4)).isInstanceOf(RatisScalarStore.UnknownOutcomeException.class);
    }

    private ObjectNode attestation()
    {
        return JsonUtils.JSON_OBJECT_MAPPER.createObjectNode().put("status", "ok").put("known", true).put("active", true)
                       .put("domain", DOMAIN.toString()).put("table", TableId.fromLong(7).asUUID().toString()).put("group", GROUP)
                       .put("generation", 3).put("schemaEpoch", 9).put("profile", "scalar-int")
                       .put("profileVersion", 1).put("protocolVersion", 1).put("storageVersion", 1).put("appliedIndex", 1);
    }

    private void reply(ObjectNode body)
    {
        response.set(new Response(200, body.toString()));
    }

    private TransactionDomainDescriptor prepared()
    {
        return new TransactionDomainDescriptor(DOMAIN, TableId.fromLong(7), "ratis-scalar", "scalar-int", 1, 1, 1, 3,
                                               Collections.singleton(new org.apache.cassandra.tcm.membership.NodeId(1)));
    }

    private void serve(HttpExchange exchange) throws IOException
    {
        exchange.getRequestBody().close();
        Response current = response.get();
        byte[] bytes = current.body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(current.status, bytes.length);
        try (OutputStream output = exchange.getResponseBody())
        {
            output.write(bytes);
        }
    }

    private static final class Response
    {
        private final int status;
        private final String body;

        private Response(int status, String body)
        {
            this.status = status;
            this.body = body;
        }
    }
}
