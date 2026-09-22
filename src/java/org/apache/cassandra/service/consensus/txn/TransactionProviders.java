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

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

/** Access to the process-local transaction provider registry. */
public final class TransactionProviders
{
    private static volatile TransactionProviderRegistry registry = defaultRegistry();

    private TransactionProviders()
    {
    }

    public static TransactionProviderRegistry registry()
    {
        return registry;
    }

    /**
     * Replace the process-local registry for tests and return the previous registry.
     * This is unsafe for concurrent ownership changes and is not durable configuration; callers must restore the
     * returned registry in a finally block.
     */
    @VisibleForTesting
    public static TransactionProviderRegistry unsafeSetRegistryForTesting(TransactionProviderRegistry replacement)
    {
        if (replacement == null)
            throw new IllegalArgumentException("Transaction provider registry must not be null");
        TransactionProviderRegistry previous = registry;
        registry = replacement;
        return previous;
    }

    private static TransactionProviderRegistry defaultRegistry()
    {
        Map<String, TransactionProvider> providers = new LinkedHashMap<>();
        providers.put(AccordTransactionProvider.ID, AccordTransactionProvider.INSTANCE);
        return new TransactionProviderRegistry(providers);
    }
}
