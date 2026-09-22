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

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability;

/**
 * Immutable provider registry with authoritative TCM ownership for reserved tables.
 */
public final class TransactionProviderRegistry
{
    private static final String DEFAULT_PROVIDER_ID = AccordTransactionProvider.ID;

    private final Map<String, ProviderEntry> providers;
    private final Map<TableId, TransactionDomain> domains;

    public TransactionProviderRegistry(Map<String, TransactionProvider> providers)
    {
        this(providers, Collections.<TableId, TransactionDomain>emptyMap().entrySet());
    }

    public TransactionProviderRegistry(Map<String, TransactionProvider> providers,
                                       Map<TableId, String> assignments)
    {
        this(providers, legacyDomains(assignments).entrySet());
    }

    /**
     * Create a registry with an explicit domain for each assigned table.
     *
     * A named factory is used instead of another Map/Map constructor because both
     * constructors would have the same erased Java signature.
     */
    public static TransactionProviderRegistry withDomains(Map<String, TransactionProvider> providers,
                                                          Map<TableId, TransactionDomain> domains)
    {
        if (domains == null)
            throw new IllegalArgumentException("Transaction table domains must not be null");

        return new TransactionProviderRegistry(providers, domains.entrySet());
    }

    private TransactionProviderRegistry(Map<String, TransactionProvider> providers,
                                       Iterable<Map.Entry<TableId, TransactionDomain>> domains)
    {
        if (providers == null)
            throw new IllegalArgumentException("Transaction providers must not be null");
        if (domains == null)
            throw new IllegalArgumentException("Transaction table domains must not be null");

        Map<String, ProviderEntry> providerEntries = new LinkedHashMap<>();
        for (Map.Entry<String, TransactionProvider> entry : providers.entrySet())
        {
            String mapId = entry.getKey();
            TransactionProvider provider = entry.getValue();
            if (mapId == null || mapId.trim().isEmpty() || provider == null)
                throw new IllegalArgumentException("Transaction provider registry contains a malformed entry");
            if (!mapId.equals(provider.id()))
                throw new IllegalArgumentException("Transaction provider map id does not match provider id: " + mapId);
            if (providerEntries.put(mapId, new ProviderEntry(provider)) != null)
                throw new IllegalArgumentException("Duplicate transaction provider id: " + mapId);
        }

        Map<TableId, TransactionDomain> domainCopy = new LinkedHashMap<>();
        Map<String, String> domainProviders = new LinkedHashMap<>();
        domainProviders.put(TransactionDomain.DEFAULT_ACCORD.id(), TransactionDomain.DEFAULT_ACCORD.providerId());
        for (Map.Entry<TableId, TransactionDomain> entry : domains)
        {
            TableId tableId = entry.getKey();
            TransactionDomain domain = entry.getValue();
            if (tableId == null || domain == null)
                throw new IllegalArgumentException("Transaction table domain contains a malformed entry");

            String existingProvider = domainProviders.putIfAbsent(domain.id(), domain.providerId());
            if (existingProvider != null && !existingProvider.equals(domain.providerId()))
                throw new IllegalArgumentException("Transaction domain id is assigned to multiple providers: " + domain.id());
            domainCopy.put(tableId, domain);
        }

        this.providers = Collections.unmodifiableMap(providerEntries);
        this.domains = Collections.unmodifiableMap(domainCopy);
    }

    private static Map<TableId, TransactionDomain> legacyDomains(Map<TableId, String> assignments)
    {
        if (assignments == null)
            throw new IllegalArgumentException("Transaction table assignments must not be null");

        Map<TableId, TransactionDomain> domains = new LinkedHashMap<>();
        for (Map.Entry<TableId, String> entry : assignments.entrySet())
        {
            if (entry.getKey() == null || entry.getValue() == null || entry.getValue().trim().isEmpty())
                throw new IllegalArgumentException("Transaction table assignment contains a malformed entry");
            domains.put(entry.getKey(), legacyDomain(entry.getValue()));
        }
        return domains;
    }

    private static TransactionDomain legacyDomain(String providerId)
    {
        return DEFAULT_PROVIDER_ID.equals(providerId)
               ? TransactionDomain.DEFAULT_ACCORD
               : new TransactionDomain(providerId, providerId);
    }

    /**
     * Resolve one provider for all referenced tables and required capabilities.
     *
     * Reserved tables use TCM ownership; unreserved tables use the local assignment snapshot or Accord.
     * an explicitly assigned but unknown provider is an error and never falls back to Accord.
     */
    public TransactionProvider resolve(Set<TableId> tables, Set<Capability> required)
    {
        return select(tables, required).provider();
    }

    /**
     * Select a provider and domain, giving durable reservations precedence over local assignments.
     */
    public Selection select(Set<TableId> tables, Set<Capability> required)
    {
        if (tables == null || required == null)
            throw new IllegalArgumentException("Transaction tables and required capabilities must not be null");

        Set<String> providerIds = new LinkedHashSet<>();
        Set<TransactionDomain> selectedDomains = new LinkedHashSet<>();
        for (TableId tableId : tables)
        {
            if (tableId == null)
                throw new IllegalArgumentException("Transaction table set contains null");
            TransactionDomain domain = domainFor(tableId);
            providerIds.add(domain.providerId());
            selectedDomains.add(domain);
        }
        if (providerIds.isEmpty())
        {
            providerIds.add(DEFAULT_PROVIDER_ID);
            selectedDomains.add(TransactionDomain.DEFAULT_ACCORD);
        }
        if (providerIds.size() != 1)
            throw new InvalidRequestException("Transaction references tables assigned to different providers: " + providerIds);
        if (selectedDomains.size() != 1)
            throw new InvalidRequestException("Transaction references tables assigned to different domains: " + selectedDomains);

        String providerId = providerIds.iterator().next();
        ProviderEntry entry = providers.get(providerId);
        if (entry == null)
            throw new InvalidRequestException("Transaction provider is not registered: " + providerId);

        for (Capability capability : required)
        {
            if (capability == null)
                throw new IllegalArgumentException("Transaction required capabilities contain null");
            if (!entry.capabilities.contains(capability))
                throw new InvalidRequestException("Transaction provider '" + providerId
                                                  + "' does not support required capability: " + capability);
        }
        return new Selection(entry.provider, selectedDomains.iterator().next());
    }

    private TransactionDomain domainFor(TableId tableId)
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata != null)
        {
            TransactionDomainDescriptor descriptor = metadata.consistencyDomains.forTable(tableId);
            if (descriptor != null)
            {
                if (descriptor.state() != TransactionDomainDescriptor.State.ACTIVE || descriptor.externalBinding() == null)
                    throw new InvalidRequestException("Transaction domain is PREPARED (closed): " + descriptor.id());
                return new TransactionDomain(descriptor.id().toString(), descriptor.providerId());
            }
        }
        return domains.getOrDefault(tableId, TransactionDomain.DEFAULT_ACCORD);
    }

    public static final class Selection
    {
        private final TransactionProvider provider;
        private final TransactionDomain domain;

        private Selection(TransactionProvider provider, TransactionDomain domain)
        {
            this.provider = provider;
            this.domain = domain;
        }

        public TransactionProvider provider()
        {
            return provider;
        }

        public TransactionDomain domain()
        {
            return domain;
        }
    }

    private static final class ProviderEntry
    {
        private final TransactionProvider provider;
        private final Set<Capability> capabilities;

        private ProviderEntry(TransactionProvider provider)
        {
            this.provider = Objects.requireNonNull(provider, "provider");
            Set<Capability> declared = provider.capabilities();
            if (declared == null)
                throw new IllegalArgumentException("Transaction provider capabilities must be non-null");
            EnumSet<Capability> capabilityCopy = EnumSet.noneOf(Capability.class);
            for (Capability capability : declared)
            {
                if (capability == null)
                    throw new IllegalArgumentException("Transaction provider capabilities must be non-null");
                capabilityCopy.add(capability);
            }
            this.capabilities = Collections.unmodifiableSet(capabilityCopy);
        }
    }
}
