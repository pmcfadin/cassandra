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
import java.util.List;

import javax.annotation.Nullable;

import accord.primitives.Keys;
import accord.primitives.Routable.Domain;
import accord.primitives.Txn;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnNamedRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnReference;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperation;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperations;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.shouldReadEphemerally;

/** Compiles the Cassandra-owned transaction program into the existing Accord representation. */
public final class AccordTransactionCompiler
{
    private AccordTransactionCompiler()
    {
    }

    /**
     * Returns {@code null} for a planned no-op.  The returned transaction is entirely Accord-owned;
     * callers must not expose it through the provider contract.
     */
    @Nullable
    public static Txn compile(TransactionPlan plan, TransactionExecutionContext context)
    {
        if (plan.isNoOp())
            return null;

        TableMetadatas.Collector tableCollector = new TableMetadatas.Collector();
        tableCollector.addAll(plan.tables().values());
        TableMetadatas.Complete tables = tableCollector.build();
        TableMetadatasAndKeys.KeyCollector keys = new TableMetadatasAndKeys.KeyCollector(tables);
        List<TxnNamedRead> reads = new ArrayList<>(plan.reads().size());
        for (TransactionPlan.Read read : plan.reads())
        {
            SinglePartitionReadCommand command = read.command();
            reads.add(new TxnNamedRead(read.slot(), keys.collect(command.metadata(), command.partitionKey()), command, tables));
        }

        List<TxnWrite.Fragment> fragments = new ArrayList<>(plan.writes().size());
        for (TransactionPlan.Write write : plan.writes())
        {
            TableMetadata table = write.baseUpdate().metadata();
            PartitionKey key = keys.collect(table, write.baseUpdate().partitionKey());
            TxnReferenceOperations operations = new TxnReferenceOperations(table,
                                                                            write.clusterings(),
                                                                            convertOperations(write.regularOperations()),
                                                                            convertOperations(write.staticOperations()));
            fragments.add(new TxnWrite.Fragment(key,
                                                write.index(),
                                                write.baseUpdate(),
                                                operations,
                                                TxnWrite.NO_TIMESTAMP));
        }

        Keys accordKeys = keys.build();
        if (accordKeys.isEmpty())
            throw new InvalidRequestException("Transaction plan has no read or write keys");

        TxnRead read = TxnRead.createTxnRead(tables,
                                             reads,
                                             fragments.isEmpty()
                                             ? readConsistency(context, tables, accordKeys)
                                             : null,
                                             Domain.Key);
        if (fragments.isEmpty())
        {
            Txn.Kind kind = shouldReadEphemerally(accordKeys,
                                                  tables.getMetadata(((PartitionKey) accordKeys.get(0)).table()).params,
                                                  Txn.Kind.Read);
            return new Txn.InMemory(kind,
                                    accordKeys,
                                    read,
                                    TxnQuery.ALL,
                                    null,
                                    new TableMetadatasAndKeys(tables, accordKeys));
        }

        ConsistencyLevel commitCL = commitConsistency(context, tables, accordKeys);
        TxnUpdate update = new TxnUpdate(tables,
                                         fragments,
                                         convertConditions(plan.conditions(), context.protocolVersion()),
                                         commitCL,
                                         PreserveTimestamp.no);
        return new Txn.InMemory(accordKeys,
                                read,
                                TxnQuery.ALL,
                                update,
                                new TableMetadatasAndKeys(tables, accordKeys));
    }

    private static List<TxnReferenceOperation> convertOperations(List<TransactionOperation> operations)
    {
        List<TxnReferenceOperation> result = new ArrayList<>(operations.size());
        for (TransactionOperation operation : operations)
        {
            result.add(new TxnReferenceOperation(TxnReferenceOperation.Kind.valueOf(operation.kind().name()),
                                                 operation.receiver(),
                                                 operation.table(),
                                                 operation.keyOrIndex(),
                                                 operation.field(),
                                                 convertValue(operation.value())));
        }
        return result;
    }

    private static TxnReferenceValue convertValue(TransactionValue value)
    {
        switch (value.kind())
        {
            case LITERAL:
                return new TxnReferenceValue.Constant(value.literal());
            case REFERENCE:
                TxnReference reference = toAccordReference(value.reference());
                if (reference.kind != TxnReference.Kind.COLUMN)
                    throw new InvalidRequestException("A deferred transaction operation must reference a column");
                return new TxnReferenceValue.Substitution(reference.asColumn());
            default:
                throw new IllegalStateException("Unhandled transaction value kind: " + value.kind());
        }
    }

    private static TxnCondition convertConditions(List<TransactionCondition> conditions, ProtocolVersion protocolVersion)
    {
        if (conditions.isEmpty())
            return TxnCondition.none();

        List<TxnCondition> converted = new ArrayList<>(conditions.size());
        for (TransactionCondition condition : conditions)
        {
            TxnReference reference = toAccordReference(condition.reference());
            switch (condition.kind())
            {
                case IS_NULL:
                case IS_NOT_NULL:
                    converted.add(new TxnCondition.Exists(reference, TxnCondition.Kind.valueOf(condition.kind().name())));
                    break;
                case EQUAL:
                case NOT_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    if (reference.kind != TxnReference.Kind.COLUMN)
                        throw new InvalidRequestException("A transaction condition must reference a column");
                    converted.add(new TxnCondition.Value(reference.asColumn(),
                                                         TxnCondition.Kind.valueOf(condition.kind().name()),
                                                         condition.value(),
                                                         protocolVersion));
                    break;
                default:
                    throw new IllegalStateException("Unhandled transaction condition kind: " + condition.kind());
            }
        }
        return converted.size() == 1 ? converted.get(0) : new TxnCondition.BooleanGroup(TxnCondition.Kind.AND, converted);
    }

    private static TxnReference toAccordReference(TransactionReference reference)
    {
        return TxnReference.columnOrRow(reference.slot(), reference.table(), reference.column(), reference.path());
    }

    private static ConsistencyLevel readConsistency(TransactionExecutionContext context,
                                                    TableMetadatas.Complete tables,
                                                    Keys keys)
    {
        if (keys.isEmpty() || context.serialConsistency() == null)
            return null;

        ClusterMetadata metadata = ClusterMetadata.current();
        for (int i = 0; i < keys.size(); i++)
        {
            PartitionKey key = (PartitionKey) keys.get(i);
            TableMetadata table = tables.getMetadata(key.table());
            TransactionalMode mode = table.params.transactionalMode;
            TransactionalMigrationFromMode migration = table.params.transactionalMigrationFrom;
            ConsistencyLevel readCL = mode.readCLForMode(migration,
                                                         context.serialConsistency(),
                                                         metadata,
                                                         key.table(),
                                                         key.token());
            if (readCL != null)
                return readCL;
        }
        return null;
    }

    private static ConsistencyLevel commitConsistency(TransactionExecutionContext context,
                                                      TableMetadatas.Complete tables,
                                                      Keys accordKeys)
    {
        ConsistencyLevel requested = context.consistency();
        if (requested == null)
            return null;

        checkArgument(!accordKeys.isEmpty(), "A write transaction must have at least one key");
        ClusterMetadata metadata = ClusterMetadata.current();
        for (int i = 0; i < accordKeys.size(); i++)
        {
            PartitionKey key = (PartitionKey) accordKeys.get(i);
            TableMetadata table = tables.getMetadata(key.table());
            TransactionalMode mode = table.params.transactionalMode;
            TransactionalMigrationFromMode migration = table.params.transactionalMigrationFrom;
            ConsistencyLevel commitCL = mode.commitCLForMode(migration,
                                                             requested,
                                                             metadata,
                                                             key.table(),
                                                             key.token());
            if (commitCL != null)
                return commitCL;
        }
        return null;
    }
}
