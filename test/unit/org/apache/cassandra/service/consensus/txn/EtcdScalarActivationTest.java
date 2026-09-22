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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.membership.NodeId;

/** Live activation race regression; skipped unless the external etcd harness is configured. */
public class EtcdScalarActivationTest
{
    @Test
    public void repeatedActivationCannotOverwriteConcurrentReceipts() throws Exception
    {
        String configured = EtcdTestEnvironment.get("ETCD_ENDPOINTS");
        Assume.assumeTrue("RUN_ETCD_TESTS", "true".equalsIgnoreCase(EtcdTestEnvironment.get("RUN_ETCD_TESTS")));
        Assume.assumeTrue(configured != null && !configured.isEmpty());
        EtcdScalarStore store = new EtcdScalarStore(Arrays.asList(configured.split(",")), 3000);
        UUID domainId = UUID.randomUUID();
        TableId tableId = TableId.fromLong(System.nanoTime());
        ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding(domainId.toString(), store.clusterId());
        TransactionDomainDescriptor prepared = new TransactionDomainDescriptor(domainId, tableId,
                                                                                EtcdScalarRequest.PROVIDER,
                                                                                EtcdScalarRequest.PROFILE, 1, 1, 1, 1,
                                                                                Set.of(new NodeId(1), new NodeId(2), new NodeId(3)));
        store.prepare(prepared, binding, 1);
        TransactionDomainDescriptor active = prepared.activate(binding);
        store.activate(active, 1);

        EtcdScalarRequest request = request(domainId, tableId, binding, UUID.randomUUID(), 1, 1, 7, 11);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        EtcdScalarResult original;
        try
        {
            List<Future<?>> activations = new ArrayList<>();
            for (int i = 0; i < 16; i++)
                activations.add(executor.submit(() -> store.activate(active, 1)));
            original = store.submit(request);
            executor.shutdown();
            for (Future<?> activation : activations)
                activation.get(20, TimeUnit.SECONDS);
            Assertions.assertThat(executor.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
            Assertions.assertThat(original.known).isTrue();
            Assertions.assertThat(store.lookup(request).encode()).isEqualTo(original.encode());
        }
        finally
        {
            executor.shutdownNow();
        }

        store.fence(domainId, 1, 2);
        Assertions.assertThat(store.lookup(request).encode()).isEqualTo(original.encode());
        Assertions.assertThatThrownBy(() -> store.activate(active, 1))
                  .isInstanceOf(IllegalStateException.class);
    }

    private static EtcdScalarRequest request(UUID domainId, TableId tableId,
                                              ExternalTransactionDomainBinding binding,
                                              UUID requestId, long generation, long epoch,
                                              int key, int value)
    {
        return new EtcdScalarRequest(domainId, tableId, requestId, generation, epoch, binding,
                                     Collections.singletonList(new EtcdScalarRequest.Read(1, key)),
                                     Collections.emptyList(),
                                     Collections.singletonList(EtcdScalarRequest.Write.constant(key, value)));
    }
}
