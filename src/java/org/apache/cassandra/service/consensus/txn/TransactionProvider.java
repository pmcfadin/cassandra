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

import java.util.Set;

/**
 * Execution provider for a bound Cassandra transaction program.
 *
 * Providers should return an immutable capability snapshot from
 * {@link #capabilities()}.
 * Reads supply the pre-write values used by conditions, deferred operations and returned references.
 * A false condition suppresses every write. Unsupported programs must be rejected before effects;
 * the registry's coarse capability checks do not replace provider-specific shape validation.
 * Returned partitions are materialized, read-only data owned by the caller.
 */
public interface TransactionProvider
{
    String id();

    Set<Capability> capabilities();

    TransactionOutcome execute(TransactionPlan plan, TransactionExecutionContext context);

    enum Capability
    {
        STRICT_SERIALIZABLE,
        READ,
        WRITE,
        CONDITIONAL,
        MULTI_TABLE
    }
}
