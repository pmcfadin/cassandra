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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability.CONDITIONAL;
import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability.READ;
import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability.STRICT_SERIALIZABLE;
import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability.WRITE;

public class TransactionProviderRegistryTest
{
    private static final TableId TABLE1 = TableId.fromString("00000000-0000-0000-0000-000000000101");
    private static final TableId TABLE2 = TableId.fromString("00000000-0000-0000-0000-000000000102");
    private ClusterMetadataService previousMetadataService;

    @Before
    public void saveMetadataService()
    {
        previousMetadataService = ClusterMetadataService.instance();
    }

    @After
    public void restoreMetadataService()
    {
        ClusterMetadataService.unsetInstance();
        if (previousMetadataService != null)
            ClusterMetadataService.setInstance(previousMetadataService);
    }

    @Test
    public void shouldUseAccordForUnassignedTablesAndHonorExplicitAssignment()
    {
        TransactionProvider accord = provider("accord", Set.of(STRICT_SERIALIZABLE, READ));
        TransactionProvider custom = provider("custom", EnumSet.allOf(TransactionProvider.Capability.class));
        Map<String, TransactionProvider> providers = new HashMap<>();
        providers.put(accord.id(), accord);
        providers.put(custom.id(), custom);

        TransactionProviderRegistry defaults = new TransactionProviderRegistry(providers, new HashMap<>());
        Assertions.assertThat(defaults.resolve(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE))).isSameAs(accord);
        Assertions.assertThat(defaults.select(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE)).domain())
                  .isSameAs(TransactionDomain.DEFAULT_ACCORD);

        TransactionProviderRegistry assigned = new TransactionProviderRegistry(providers, Map.of(TABLE1, custom.id()));
        Assertions.assertThat(assigned.resolve(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE))).isSameAs(custom);
        Assertions.assertThat(assigned.select(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE)).domain())
                  .isEqualTo(new TransactionDomain(custom.id(), custom.id()));
    }

    @Test
    public void shouldRejectTablesWithDifferentEffectiveProviders()
    {
        TransactionProvider accord = provider("accord", EnumSet.allOf(TransactionProvider.Capability.class));
        TransactionProvider custom = provider("custom", EnumSet.allOf(TransactionProvider.Capability.class));
        TransactionProviderRegistry registry = new TransactionProviderRegistry(Map.of(accord.id(), accord, custom.id(), custom),
                                                                                Map.of(TABLE2, custom.id()));

        Assertions.assertThatThrownBy(() -> registry.resolve(Set.of(TABLE1, TABLE2), Set.of(STRICT_SERIALIZABLE)))
                  .isInstanceOf(org.apache.cassandra.exceptions.InvalidRequestException.class)
                  .hasMessageContaining("different providers");
    }

    @Test
    public void shouldRejectTablesWithSameProviderAndDifferentDomains()
    {
        TransactionProvider custom = provider("custom", EnumSet.allOf(TransactionProvider.Capability.class));
        TransactionProviderRegistry registry = TransactionProviderRegistry.withDomains(
                Map.of(custom.id(), custom),
                Map.of(TABLE1, new TransactionDomain("left", custom.id()),
                       TABLE2, new TransactionDomain("right", custom.id())));

        Assertions.assertThatThrownBy(() -> registry.select(Set.of(TABLE1, TABLE2), Set.of(STRICT_SERIALIZABLE)))
                  .isInstanceOf(org.apache.cassandra.exceptions.InvalidRequestException.class)
                  .hasMessageContaining("different domains");
    }

    @Test
    public void shouldRejectAmbiguousDomainDescriptor()
    {
        TransactionProvider first = provider("first", EnumSet.allOf(TransactionProvider.Capability.class));
        TransactionProvider second = provider("second", EnumSet.allOf(TransactionProvider.Capability.class));

        Assertions.assertThatThrownBy(() -> TransactionProviderRegistry.withDomains(
                Map.of(first.id(), first, second.id(), second),
                Map.of(TABLE1, new TransactionDomain("shared", first.id()),
                       TABLE2, new TransactionDomain("shared", second.id()))))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("multiple providers");

        TransactionProvider custom = provider("custom", EnumSet.allOf(TransactionProvider.Capability.class));
        Assertions.assertThatThrownBy(() -> TransactionProviderRegistry.withDomains(
                Map.of(custom.id(), custom),
                Map.of(TABLE1, new TransactionDomain(TransactionDomain.DEFAULT_ACCORD.id(), custom.id()))))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("multiple providers");
    }

    @Test
    public void shouldExposeImmutableExecutionContextFacts()
    {
        TransactionDomain domain = new TransactionDomain("test", "custom");
        Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
        TransactionExecutionContext context = new TransactionExecutionContext(domain,
                                                                                ConsistencyLevel.QUORUM,
                                                                                ConsistencyLevel.SERIAL,
                                                                                ProtocolVersion.CURRENT,
                                                                                requestTime);

        Assertions.assertThat(context.domain()).isSameAs(domain);
        Assertions.assertThat(context.consistency()).isEqualTo(ConsistencyLevel.QUORUM);
        Assertions.assertThat(context.serialConsistency()).isEqualTo(ConsistencyLevel.SERIAL);
        Assertions.assertThat(context.protocolVersion()).isSameAs(ProtocolVersion.CURRENT);
        Assertions.assertThat(context.requestTime()).isSameAs(requestTime);

        TransactionExecutionContext internal = new TransactionExecutionContext(domain,
                                                                                 null,
                                                                                 ConsistencyLevel.SERIAL,
                                                                                 ProtocolVersion.CURRENT,
                                                                                 requestTime);
        Assertions.assertThat(internal.consistency()).isNull();
        Assertions.assertThat(internal.serialConsistency()).isEqualTo(ConsistencyLevel.SERIAL);
    }

    @Test
    public void shouldRejectUnknownProviderWithoutFallingBackToAccord()
    {
        TransactionProvider accord = provider("accord", EnumSet.allOf(TransactionProvider.Capability.class));
        TransactionProviderRegistry registry = new TransactionProviderRegistry(Map.of(accord.id(), accord),
                                                                                Map.of(TABLE1, "missing"));

        Assertions.assertThatThrownBy(() -> registry.resolve(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE)))
                  .isInstanceOf(org.apache.cassandra.exceptions.InvalidRequestException.class)
                  .hasMessageContaining("not registered");
    }

    @Test
    public void shouldUseTcmAssignmentWhenProviderIsMissing()
    {
        TransactionProvider accord = provider("accord", EnumSet.allOf(TransactionProvider.Capability.class));
        installTcmDomain(true);
        TransactionProviderRegistry registry = new TransactionProviderRegistry(Map.of(accord.id(), accord));

        Assertions.assertThatThrownBy(() -> registry.resolve(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE)))
                  .isInstanceOf(org.apache.cassandra.exceptions.InvalidRequestException.class)
                  .hasMessageContaining("not registered");
    }

    @Test
    public void shouldNotLetExplicitAssignmentOverrideTcmDomain()
    {
        TransactionProvider accord = provider("accord", EnumSet.allOf(TransactionProvider.Capability.class));
        TransactionProvider custom = provider("etcd-scalar", EnumSet.allOf(TransactionProvider.Capability.class));
        installTcmDomain(true);
        TransactionProviderRegistry registry = new TransactionProviderRegistry(Map.of(accord.id(), accord, custom.id(), custom),
                                                                                Map.of(TABLE1, accord.id()));

        Assertions.assertThat(registry.resolve(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE))).isSameAs(custom);
    }

    @Test
    public void preparedDomainCannotBeSelectedEvenWithRegisteredProvider()
    {
        TransactionProvider custom = provider("etcd-scalar", EnumSet.allOf(TransactionProvider.Capability.class));
        installTcmDomain(false);
        TransactionProviderRegistry registry = new TransactionProviderRegistry(Map.of(custom.id(), custom));
        Assertions.assertThatThrownBy(() -> registry.resolve(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE)))
                  .isInstanceOf(org.apache.cassandra.exceptions.InvalidRequestException.class)
                  .hasMessageContaining("PREPARED (closed)");
    }

    private void installTcmDomain(boolean active)
    {
        TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(
                java.util.UUID.fromString("00000000-0000-0000-0000-000000000701"), TABLE1,
                "etcd-scalar", "scalar-int", 1, 1, 1, 1, Set.of(new NodeId(1)));
        if (active)
            descriptor = descriptor.activate(new ExternalTransactionDomainBinding("00000000-0000-0000-0000-000000000702", "42"));
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY)
                                   .transformer()
                                   .with(new ConsistencyDomains(Epoch.FIRST, Map.of(TABLE1, descriptor)))
                                   .build().metadata;
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(metadata.forceEpoch(Epoch.EMPTY)));
    }

    @Test
    public void shouldRejectMissingCapabilitiesBeforeProviderInvocation()
    {
        AtomicInteger invocations = new AtomicInteger();
        TransactionProvider readOnly = provider("read-only", EnumSet.of(STRICT_SERIALIZABLE, READ), invocations);
        TransactionProviderRegistry registry = new TransactionProviderRegistry(Map.of(readOnly.id(), readOnly),
                                                                                Map.of(TABLE1, readOnly.id()));

        Assertions.assertThatThrownBy(() -> registry.resolve(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE, WRITE)))
                  .isInstanceOf(org.apache.cassandra.exceptions.InvalidRequestException.class)
                  .hasMessageContaining("WRITE");
        Assertions.assertThat(invocations).hasValue(0);
    }

    @Test
    public void shouldSnapshotProviderAndAssignmentInputs()
    {
        Set<TransactionProvider.Capability> capabilities = EnumSet.of(STRICT_SERIALIZABLE, READ, WRITE, CONDITIONAL);
        AtomicInteger invocations = new AtomicInteger();
        TransactionProvider custom = provider("custom", capabilities, invocations);
        Map<String, TransactionProvider> providers = new HashMap<>();
        providers.put(custom.id(), custom);
        Map<TableId, String> assignments = new HashMap<>();
        assignments.put(TABLE1, custom.id());

        TransactionProviderRegistry registry = new TransactionProviderRegistry(providers, assignments);
        providers.clear();
        assignments.clear();
        capabilities.clear();

        Assertions.assertThat(registry.resolve(Set.of(TABLE1), Set.of(STRICT_SERIALIZABLE, WRITE))).isSameAs(custom);
    }

    @Test
    public void shouldRejectMalformedProviderIds()
    {
        TransactionProvider provider = provider("actual", EnumSet.of(STRICT_SERIALIZABLE));

        Assertions.assertThatThrownBy(() -> new TransactionProviderRegistry(Map.of("different", provider), Map.of()))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("does not match");
        Assertions.assertThatThrownBy(() -> new TransactionProviderRegistry(Map.of("", provider), Map.of()))
                  .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> new TransactionProviderRegistry(Map.of(provider.id(), provider),
                                                                            Map.of(TABLE1, " ")))
                  .isInstanceOf(IllegalArgumentException.class);
    }

    private static TransactionProvider provider(String id, Set<TransactionProvider.Capability> capabilities)
    {
        return provider(id, capabilities, new AtomicInteger());
    }

    private static TransactionProvider provider(String id,
                                                Set<TransactionProvider.Capability> capabilities,
                                                AtomicInteger invocations)
    {
        return new TransactionProvider()
        {
            @Override
            public String id()
            {
                return id;
            }

            @Override
            public Set<Capability> capabilities()
            {
                return capabilities;
            }

            @Override
            public TransactionOutcome execute(TransactionPlan plan,
                                              TransactionExecutionContext context)
            {
                invocations.incrementAndGet();
                return TransactionOutcome.noOp();
            }
        };
    }
}
