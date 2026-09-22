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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;

public class RatisTransactionCompileTest
{
    private static final String KEYSPACE = "ratis_compile";
    private static final String TABLE = "values";
    private static final TableId TABLE_ID = TableId.fromString("00000000-0000-0000-0000-000000000901");
    private static final UUID DOMAIN_ID = UUID.fromString("00000000-0000-0000-0000-000000000902");
    private static final String GROUP_ID = "00000000-0000-0000-0000-000000000903";

    private ClusterMetadataService previousMetadataService;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE " + TABLE + " (pk int PRIMARY KEY, v int) WITH transactional_mode = 'full'", KEYSPACE)
                                     .id(TABLE_ID).build();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), table);
    }

    @Before
    public void installActiveMetadata()
    {
        previousMetadataService = ClusterMetadataService.instance();
        TableMetadata table = TransactionDomainGuard.markReserved(table(), DOMAIN_ID);
        TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(DOMAIN_ID, TABLE_ID,
                                                                                   RatisTransactionProvider.ID,
                                                                                   "scalar-int", 1, 1, 1, 1,
                                                                                   Collections.singleton(new NodeId(1)))
                                                    .activate(ExternalTransactionDomainBinding.ratis(GROUP_ID, table.epoch.getEpoch(), 1));
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(table));
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY,
                                                       new DistributedSchema(ClusterMetadata.current().schema.getKeyspaces().withAddedOrUpdated(keyspace)))
                                   .transformer()
                                   .with(new ConsistencyDomains(Epoch.FIRST, Collections.singletonMap(TABLE_ID, descriptor)))
                                   .build().metadata;
        metadata = metadata.forceEpoch(Epoch.EMPTY);
        metadata.schema.initializeKeyspaceInstances(ClusterMetadata.current().schema, false);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(metadata));
    }

    @After
    public void restoreMetadata()
    {
        ClusterMetadataService.unsetInstance();
        if (previousMetadataService != null)
            ClusterMetadataService.setInstance(previousMetadataService);
    }

    @Test
    public void compilesConstantWriteWithStableIdentityFields()
    {
        RatisScalarRequest request = compile("BEGIN TRANSACTION\n" +
                                            "  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1;\n" +
                                            "COMMIT TRANSACTION;");

        Assertions.assertThat(request.domainId).isEqualTo(DOMAIN_ID);
        Assertions.assertThat(request.tableId).isEqualTo(TABLE_ID);
        Assertions.assertThat(request.groupId).isEqualTo(GROUP_ID);
        Assertions.assertThat(request.atMicros).isGreaterThanOrEqualTo(0L);
        Assertions.assertThat(request.writes).anyMatch(write -> Integer.valueOf(7).equals(write.constant));
    }

    @Test
    public void compilesReferenceCopyAndConditions()
    {
        RatisScalarRequest request = compile("BEGIN TRANSACTION\n" +
                                            "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1);\n" +
                                            "  IF source.v = 4 AND source IS NOT NULL THEN\n" +
                                            "    UPDATE " + KEYSPACE + '.' + TABLE + " SET v = source.v WHERE pk = 2;\n" +
                                            "  END IF\n" +
                                            "COMMIT TRANSACTION;");

        Assertions.assertThat(request.conditions).hasSize(2);
        Assertions.assertThat(request.conditions).anyMatch(condition -> condition.kind == TransactionCondition.Kind.EQUAL && condition.value == 4);
        Assertions.assertThat(request.conditions).anyMatch(condition -> condition.rowReference && condition.kind == TransactionCondition.Kind.IS_NOT_NULL);
        Assertions.assertThat(request.writes).anyMatch(write -> write.constant == null && write.sourceSlot >= 0);
    }

    @Test
    public void recompilationWithStableIdentityAndTimestampIsByteIdentical()
    {
        TransactionPlan plan = plan("BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1; COMMIT TRANSACTION;");
        UUID requestId = UUID.fromString("00000000-0000-0000-0000-000000000904");
        long atMicros = 123456789L;
        RatisTransactionProvider provider = provider();

        RatisScalarRequest first = provider.compile(plan, context(ConsistencyLevel.ONE), requestId, atMicros);
        RatisScalarRequest second = provider.compile(plan, context(ConsistencyLevel.ONE), requestId, atMicros);

        Assertions.assertThat(Arrays.equals(first.encode(), second.encode())).isTrue();
    }

    @Test
    public void changingValuesChangesEncodedProgram()
    {
        UUID requestId = UUID.fromString("00000000-0000-0000-0000-000000000904");
        long atMicros = 123456789L;
        RatisTransactionProvider provider = provider();
        RatisScalarRequest seven = provider.compile(plan("BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1; COMMIT TRANSACTION;"),
                                                     context(ConsistencyLevel.ONE), requestId, atMicros);
        RatisScalarRequest eight = provider.compile(plan("BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 8 WHERE pk = 1; COMMIT TRANSACTION;"),
                                                     context(ConsistencyLevel.ONE), requestId, atMicros);

        Assertions.assertThat(Arrays.equals(seven.encode(), eight.encode())).isFalse();
    }

    @Test
    public void changingProjectionChangesEncodedProgram()
    {
        UUID requestId = UUID.fromString("00000000-0000-0000-0000-000000000904");
        long atMicros = 123456789L;
        RatisTransactionProvider provider = provider();
        RatisScalarRequest valueOnly = provider.compile(plan("BEGIN TRANSACTION SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1; COMMIT TRANSACTION;"),
                                                         context(ConsistencyLevel.ONE), requestId, atMicros);
        RatisScalarRequest keyAndValue = provider.compile(plan("BEGIN TRANSACTION SELECT pk, v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1; COMMIT TRANSACTION;"),
                                                           context(ConsistencyLevel.ONE), requestId, atMicros);

        Assertions.assertThat(valueOnly.returnShape).isNotEqualTo(keyAndValue.returnShape);
        Assertions.assertThat(Arrays.equals(valueOnly.encode(), keyAndValue.encode())).isFalse();
    }

    @Test
    public void rejectsWrongProviderAndConsistencyBeforeTransport()
    {
        TransactionPlan plan = plan("BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1; COMMIT TRANSACTION;");
        RatisTransactionProvider provider = provider();
        TransactionExecutionContext wrongProvider = new TransactionExecutionContext(new TransactionDomain(DOMAIN_ID.toString(), "etcd-scalar"),
                                                                                    ConsistencyLevel.ONE, ConsistencyLevel.SERIAL,
                                                                                    ProtocolVersion.CURRENT, Dispatcher.RequestTime.forImmediateExecution());
        Assertions.assertThatThrownBy(() -> provider.compile(plan, wrongProvider, UUID.randomUUID()))
                  .isInstanceOf(InvalidRequestException.class);
        Assertions.assertThatThrownBy(() -> provider.compile(plan, context(ConsistencyLevel.QUORUM), UUID.randomUUID()))
                  .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void rejectsWrongGroupBeforeTransport()
    {
        RatisScalarRequest request = compile("BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1; COMMIT TRANSACTION;");
        RatisScalarRequest wrong = new RatisScalarRequest(request.domainId, request.tableId, request.requestId,
                                                          request.generation, request.schemaEpoch, UUID.randomUUID().toString(),
                                                          request.atMicros, request.reads, request.conditions, request.writes,
                                                          request.returnShape);
        Assertions.assertThatThrownBy(() -> provider().lookup(wrong))
                  .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void rejectsWrongIdentityGenerationAndTableBeforeTransport()
    {
        RatisScalarRequest request = compile("BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1; COMMIT TRANSACTION;");
        RatisScalarRequest wrongDomain = new RatisScalarRequest(UUID.randomUUID(), request.tableId, request.requestId,
                                                                request.generation, request.schemaEpoch, request.groupId,
                                                                request.atMicros, request.reads, request.conditions, request.writes,
                                                                request.returnShape);
        RatisScalarRequest wrongGeneration = new RatisScalarRequest(request.domainId, request.tableId, request.requestId,
                                                                     request.generation + 1, request.schemaEpoch, request.groupId,
                                                                     request.atMicros, request.reads, request.conditions, request.writes,
                                                                     request.returnShape);
        RatisScalarRequest wrongTable = new RatisScalarRequest(request.domainId, TableId.fromLong(9999), request.requestId,
                                                                request.generation, request.schemaEpoch, request.groupId,
                                                                request.atMicros, request.reads, request.conditions, request.writes,
                                                                request.returnShape);

        Assertions.assertThatThrownBy(() -> provider().lookup(wrongDomain)).isInstanceOf(InvalidRequestException.class);
        Assertions.assertThatThrownBy(() -> provider().lookup(wrongGeneration)).isInstanceOf(InvalidRequestException.class);
        Assertions.assertThatThrownBy(() -> provider().lookup(wrongTable)).isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void rejectsNegativeFixedTimestamp()
    {
        TransactionPlan plan = plan("BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1; COMMIT TRANSACTION;");
        Assertions.assertThatThrownBy(() -> provider().compile(plan, context(ConsistencyLevel.ONE), UUID.randomUUID(), -1L))
                  .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void rejectsStaleSchemaBeforeTransport()
    {
        RatisScalarRequest request = compile("BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1; COMMIT TRANSACTION;");
        RatisScalarRequest stale = new RatisScalarRequest(request.domainId, request.tableId, request.requestId,
                                                          request.generation, request.schemaEpoch + 1, request.groupId,
                                                          request.atMicros, request.reads, request.conditions, request.writes,
                                                          request.returnShape);
        Assertions.assertThatThrownBy(() -> provider().lookup(stale))
                  .isInstanceOf(InvalidRequestException.class);
    }

    private RatisScalarRequest compile(String query)
    {
        return provider().compile(plan(query), context(ConsistencyLevel.ONE),
                                  UUID.fromString("00000000-0000-0000-0000-000000000904"));
    }

    private static RatisTransactionProvider provider()
    {
        return new RatisTransactionProvider(new RatisScalarStore(Collections.singletonList("http://127.0.0.1:1"), 25));
    }

    private static TransactionExecutionContext context(ConsistencyLevel consistency)
    {
        return new TransactionExecutionContext(new TransactionDomain(DOMAIN_ID.toString(), RatisTransactionProvider.ID),
                                                consistency, ConsistencyLevel.SERIAL,
                                                ProtocolVersion.CURRENT, Dispatcher.RequestTime.forImmediateExecution());
    }

    private static TransactionPlan plan(String query)
    {
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        CQLStatement statement = parsed.prepare(ClientState.forInternalCalls());
        TransactionStatement transaction = (TransactionStatement) statement;
        QueryOptions options = QueryProcessor.makeInternalOptions(transaction, new Object[0]);
        return transaction.toPlan(ClientState.forInternalCalls(), options);
    }

    private static TableMetadata table()
    {
        return org.apache.cassandra.schema.Schema.instance.getTableMetadata(KEYSPACE, TABLE);
    }
}
