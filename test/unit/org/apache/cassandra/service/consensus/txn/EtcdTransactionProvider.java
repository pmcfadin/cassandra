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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability;
import org.apache.cassandra.tcm.ClusterMetadata;

/** Adapter which turns Cassandra's bounded scalar plan into an etcd receipt. */
public final class EtcdTransactionProvider implements TransactionProvider
{
    public static final String ID = EtcdScalarRequest.PROVIDER;
    private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(EnumSet.of(Capability.STRICT_SERIALIZABLE,
                                                                                              Capability.READ,
                                                                                              Capability.WRITE,
                                                                                              Capability.CONDITIONAL));
    private final EtcdScalarStore store;

    public EtcdTransactionProvider(EtcdScalarStore store)
    {
        this.store = store;
    }

    @Override
    public String id() { return ID; }

    @Override
    public Set<Capability> capabilities() { return CAPABILITIES; }

    public EtcdScalarRequest compile(TransactionPlan plan, TransactionExecutionContext context, UUID requestId)
    {
        if (plan == null || context == null || requestId == null)
            throw new InvalidRequestException("etcd-scalar requires a plan, context and request id");
        if (context.consistency() != ConsistencyLevel.ONE || context.serialConsistency() != ConsistencyLevel.SERIAL)
            throw new InvalidRequestException("etcd-scalar requires consistency ONE and serial consistency SERIAL");
        if (plan.tables().size() != 1)
            throw new InvalidRequestException("etcd-scalar supports exactly one table");
        TableMetadata table = plan.tables().values().iterator().next();
        validateAdmission(table, context);
        ScalarTransactionPlan scalar = ScalarTransactionPlan.compile(plan, context);
        List<EtcdScalarRequest.Read> reads = new java.util.ArrayList<>();
        for (ScalarTransactionPlan.Read read : scalar.reads)
            reads.add(new EtcdScalarRequest.Read(read.slot, read.key));
        List<EtcdScalarRequest.Condition> conditions = new java.util.ArrayList<>();
        for (ScalarTransactionPlan.Condition condition : scalar.conditions)
            conditions.add(new EtcdScalarRequest.Condition(condition.slot,
                                                           condition.rowReference,
                                                           operator(condition.kind),
                                                           condition.value));
        List<EtcdScalarRequest.Write> writes = new java.util.ArrayList<>();
        for (ScalarTransactionPlan.Write write : scalar.writes)
            writes.add(write.constant == null ? EtcdScalarRequest.Write.reference(write.key, write.sourceSlot)
                                              : EtcdScalarRequest.Write.constant(write.key, write.constant));
        ExternalTransactionDomainBinding binding = activeBinding(table, context);
        return new EtcdScalarRequest(UUID.fromString(context.domain().id()), table.id, requestId,
                                     descriptor(table).generation(), plan.minEpoch(), binding,
                                     reads, conditions, writes, scalar.returnShape);
    }

    public EtcdScalarResult submit(EtcdScalarRequest request)
    {
        validateRequestAdmission(request);
        return store.submit(request);
    }

    public EtcdScalarResult lookup(EtcdScalarRequest request)
    {
        validateRequestAdmission(request);
        return store.lookup(request);
    }

    @Override
    public TransactionOutcome execute(TransactionPlan plan, TransactionExecutionContext context)
    {
        EtcdScalarRequest request = compile(plan, context, UUID.randomUUID());
        if (plan.isNoOp())
            return TransactionOutcome.noOp();
        EtcdScalarResult result = submit(request);
        if (!result.known)
            throw new EtcdScalarStore.UnknownOutcomeException("etcd-scalar returned an unknown outcome", null);
        Map<Integer, Integer> reads = new java.util.LinkedHashMap<>();
        for (TransactionPlan.Read read : plan.reads())
        {
            EtcdScalarResult.Row row = result.reads.get(read.slot());
            if (row != null)
                reads.put(read.slot(), row.present ? row.value : null);
        }
        return ScalarTransactionPlan.materialize(plan, result.atMicros, reads);
    }

    private static EtcdScalarRequest.Operator operator(TransactionCondition.Kind kind)
    {
        switch (kind)
        {
            case IS_NULL: return EtcdScalarRequest.Operator.IS_NULL;
            case IS_NOT_NULL: return EtcdScalarRequest.Operator.IS_NOT_NULL;
            case EQUAL: return EtcdScalarRequest.Operator.EQ;
            case NOT_EQUAL: return EtcdScalarRequest.Operator.NEQ;
            case LESS_THAN: return EtcdScalarRequest.Operator.LT;
            case LESS_THAN_OR_EQUAL: return EtcdScalarRequest.Operator.LTE;
            case GREATER_THAN: return EtcdScalarRequest.Operator.GT;
            case GREATER_THAN_OR_EQUAL: return EtcdScalarRequest.Operator.GTE;
            default: throw new InvalidRequestException("unsupported condition");
        }
    }

    private static void validateAdmission(TableMetadata table, TransactionExecutionContext context)
    {
        TransactionDomainGuard.checkTransaction(table, "etcd-scalar execution");
        if (!context.domain().providerId().equals(ID))
            throw new InvalidRequestException("wrong transaction provider domain");
    }

    private static TransactionDomainDescriptor descriptor(TableMetadata table)
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        TransactionDomainDescriptor descriptor = metadata == null ? null : metadata.consistencyDomains.forTable(table.id);
        if (descriptor == null || descriptor.state() != TransactionDomainDescriptor.State.ACTIVE || descriptor.externalBinding() == null)
            throw new InvalidRequestException("etcd-scalar domain is not ACTIVE");
        return descriptor;
    }

    private static ExternalTransactionDomainBinding activeBinding(TableMetadata table, TransactionExecutionContext context)
    {
        TransactionDomainDescriptor descriptor = descriptor(table);
        if (!descriptor.id().toString().equals(context.domain().id())
            || !ID.equals(descriptor.providerId())
            || !EtcdScalarRequest.PROFILE.equals(descriptor.profileId())
            || descriptor.profileVersion() != 1
            || descriptor.protocolVersion() != 1
            || descriptor.storageVersion() != 1
            || descriptor.externalBinding().kind() != ExternalTransactionDomainBinding.Kind.ETCD_CLUSTER)
            throw new InvalidRequestException("etcd-scalar descriptor does not match request");
        return descriptor.externalBinding();
    }

    private static void validateRequestAdmission(EtcdScalarRequest request)
    {
        if (request == null)
            throw new InvalidRequestException("etcd-scalar request is missing");
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null)
            throw new InvalidRequestException("etcd-scalar requires authoritative transaction metadata");
        TransactionDomainDescriptor descriptor = metadata.consistencyDomains.forTable(request.tableId);
        TableMetadata table = metadata.schema.getKeyspaces().getTableOrViewNullable(request.tableId);
        if (table == null || descriptor == null || descriptor.state() != TransactionDomainDescriptor.State.ACTIVE
            || descriptor.externalBinding() == null || !descriptor.id().equals(request.domainId)
            || !ID.equals(descriptor.providerId()) || descriptor.generation() != request.generation
            || !descriptor.externalBinding().equals(request.binding)
            || descriptor.protocolVersion() != 1
            || descriptor.profileVersion() != 1
            || descriptor.storageVersion() != 1
            || !EtcdScalarRequest.PROFILE.equals(descriptor.profileId())
            || descriptor.externalBinding().kind() != ExternalTransactionDomainBinding.Kind.ETCD_CLUSTER
            || table.epoch.getEpoch() != request.schemaEpoch)
            throw new InvalidRequestException("etcd-scalar request no longer matches authoritative metadata");
        TransactionDomainGuard.checkTransaction(table, "etcd-scalar execution");
    }
}
