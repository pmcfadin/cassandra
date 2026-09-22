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

import java.util.Objects;

/**
 * Process-local ownership identity for a transaction domain.
 *
 * The domain is deliberately separate from a provider's capability set.  It does
 * not represent a persisted generation, a topology fence, or readiness on any
 * other node.
 */
public final class TransactionDomain
{
    public static final TransactionDomain DEFAULT_ACCORD = new TransactionDomain("accord", AccordTransactionProvider.ID);

    private final String id;
    private final String providerId;

    public TransactionDomain(String id, String providerId)
    {
        if (id == null || id.trim().isEmpty())
            throw new IllegalArgumentException("Transaction domain id must not be blank");
        if (providerId == null || providerId.trim().isEmpty())
            throw new IllegalArgumentException("Transaction domain provider id must not be blank");

        this.id = id;
        this.providerId = providerId;
    }

    public String id()
    {
        return id;
    }

    public String providerId()
    {
        return providerId;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof TransactionDomain))
            return false;
        TransactionDomain that = (TransactionDomain) other;
        return id.equals(that.id) && providerId.equals(that.providerId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, providerId);
    }

    @Override
    public String toString()
    {
        return "TransactionDomain{" + id + ", provider=" + providerId + '}';
    }
}
