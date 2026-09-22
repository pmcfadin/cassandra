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
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

/** Test-only Cassandra adapter for a single-owner Ratis scalar group. */
public final class RatisTransactionProvider implements TransactionProvider
{
    public static final String ID = "ratis-scalar";
    private static final String PROFILE = "scalar-int";
    private static final int VERSION = 1;
    private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(EnumSet.of(Capability.STRICT_SERIALIZABLE,
                                                                                              Capability.READ,
                                                                                              Capability.WRITE,
                                                                                              Capability.CONDITIONAL));

    private final RatisScalarStore store;

    public RatisTransactionProvider(RatisScalarStore store)
    {
        if (store == null)
            throw new IllegalArgumentException("ratis-scalar store is required");
        this.store = store;
    }

    @Override
    public String id()
    {
        return ID;
    }

    @Override
    public Set<Capability> capabilities()
    {
        return CAPABILITIES;
    }

    public RatisScalarRequest compile(TransactionPlan plan, TransactionExecutionContext context, UUID requestId)
    {
        return compile(plan, context, requestId, FBUtilities.timestampMicros());
    }

    public RatisScalarRequest compile(TransactionPlan plan, TransactionExecutionContext context, UUID requestId, long atMicros)
    {
        if (plan == null || context == null || requestId == null)
            throw new InvalidRequestException("ratis-scalar requires a plan, context and request id");
        if (atMicros < 0)
            throw new InvalidRequestException("ratis-scalar timestamp must be nonnegative");
        TableMetadata table = admittedTable(plan, context);
        ScalarTransactionPlan compiled = ScalarTransactionPlan.compile(plan, context);
        TransactionDomainDescriptor descriptor = descriptor(table);
        ExternalTransactionDomainBinding binding = descriptor.externalBinding();
        return new RatisScalarRequest(descriptor.id(), table.id, requestId,
                                      descriptor.generation(), table.epoch.getEpoch(),
                                      binding.groupId(), atMicros,
                                      compiled.reads, compiled.conditions, compiled.writes,
                                      compiled.returnShape);
    }

    public RatisScalarResult submit(RatisScalarRequest request)
    {
        validateRequestAdmission(request);
        return store.submit(request);
    }

    public RatisScalarResult lookup(RatisScalarRequest request)
    {
        validateRequestAdmission(request);
        return store.lookup(request);
    }

    @Override
    public TransactionOutcome execute(TransactionPlan plan, TransactionExecutionContext context)
    {
        RatisScalarRequest request = compile(plan, context, UUID.randomUUID());
        if (plan.isNoOp())
            return TransactionOutcome.noOp();
        RatisScalarResult result = submit(request);
        if (!result.known)
            throw new RatisScalarStore.UnknownOutcomeException("ratis-scalar returned an unknown outcome", null);
        return ScalarTransactionPlan.materialize(plan, result.atMicros, result.reads);
    }

    private static TableMetadata admittedTable(TransactionPlan plan, TransactionExecutionContext context)
    {
        if (context.consistency() != ConsistencyLevel.ONE || context.serialConsistency() != ConsistencyLevel.SERIAL)
            throw new InvalidRequestException("ratis-scalar requires consistency ONE and serial consistency SERIAL");
        if (!ID.equals(context.domain().providerId()))
            throw new InvalidRequestException("wrong transaction provider domain");
        if (plan.tables().size() != 1)
            throw new InvalidRequestException("ratis-scalar supports exactly one table");
        TableMetadata table = plan.tables().values().iterator().next();
        validateAdmission(table, context);
        return table;
    }

    private static TransactionDomainDescriptor descriptor(TableMetadata table)
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        TransactionDomainDescriptor descriptor = metadata == null ? null : metadata.consistencyDomains.forTable(table.id);
        if (descriptor == null || descriptor.state() != TransactionDomainDescriptor.State.ACTIVE
            || descriptor.externalBinding() == null)
            throw new InvalidRequestException("ratis-scalar domain is not ACTIVE");
        return descriptor;
    }

    private static void validateAdmission(TableMetadata table, TransactionExecutionContext context)
    {
        TransactionDomainGuard.checkTransaction(table, "ratis-scalar execution");
        TransactionDomainDescriptor descriptor = descriptor(table);
        ExternalTransactionDomainBinding binding = descriptor.externalBinding();
        if (!descriptor.id().toString().equals(context.domain().id())
            || !ID.equals(descriptor.providerId())
            || !PROFILE.equals(descriptor.profileId())
            || descriptor.profileVersion() != VERSION
            || descriptor.protocolVersion() != VERSION
            || descriptor.storageVersion() != VERSION
            || binding.kind() != ExternalTransactionDomainBinding.Kind.RATIS_GROUP
            || binding.schemaEpoch() != table.epoch.getEpoch()
            || binding.readinessIndex() <= 0)
            throw new InvalidRequestException("ratis-scalar descriptor does not match request");
    }

    private static void validateRequestAdmission(RatisScalarRequest request)
    {
        if (request == null)
            throw new InvalidRequestException("ratis-scalar request is missing");
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null)
            throw new InvalidRequestException("ratis-scalar requires authoritative transaction metadata");
        TransactionDomainDescriptor descriptor = metadata.consistencyDomains.forTable(request.tableId);
        TableMetadata table = metadata.schema.getKeyspaces().getTableOrViewNullable(request.tableId);
        if (table == null || descriptor == null || descriptor.state() != TransactionDomainDescriptor.State.ACTIVE
            || descriptor.externalBinding() == null || !descriptor.id().equals(request.domainId)
            || !ID.equals(descriptor.providerId()) || !PROFILE.equals(descriptor.profileId())
            || descriptor.profileVersion() != VERSION || descriptor.protocolVersion() != VERSION
            || descriptor.storageVersion() != VERSION || descriptor.generation() != request.generation
            || table.epoch.getEpoch() != request.schemaEpoch)
            throw new InvalidRequestException("ratis-scalar request no longer matches authoritative metadata");

        ExternalTransactionDomainBinding binding = descriptor.externalBinding();
        if (binding.kind() != ExternalTransactionDomainBinding.Kind.RATIS_GROUP
            || !binding.groupId().equals(request.groupId)
            || binding.schemaEpoch() != request.schemaEpoch
            || binding.schemaEpoch() != table.epoch.getEpoch()
            || binding.readinessIndex() <= 0)
            throw new InvalidRequestException("ratis-scalar request has the wrong group binding");
        TransactionDomainGuard.checkTransaction(table, "ratis-scalar execution");
    }
}
