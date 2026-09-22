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

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;

public class ScalarTransactionProviderTest
{
    private static final String KEYSPACE = "scalar_txn_provider";
    private static final String TABLE = "scalar_values";
    private static final TableId TABLE_ID = TableId.fromString("00000000-0000-0000-0000-000000000301");

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse(
                                        "CREATE TABLE " + TABLE + " (pk int PRIMARY KEY, v int) WITH transactional_mode = 'full'",
                                        KEYSPACE).id(TABLE_ID));
    }

    @Test
    public void shouldReadAndApplyAConditionalConstantWriteFromAParsedPlan()
    {
        TableMetadata table = table();
        ScalarTransactionProvider provider = new ScalarTransactionProvider();
        provider.seed(table, 1, 4);

        TransactionPlan plan = plan("BEGIN TRANSACTION\n" +
                                    "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1);\n" +
                                    "  IF source.v = 4 THEN\n" +
                                    "    UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 2;\n" +
                                    "  END IF\n" +
                                    "COMMIT TRANSACTION;");

        TransactionOutcome outcome = provider.execute(plan, context());

        Assertions.assertThat(provider.value(table, 2)).isEqualTo(7);
        Assertions.assertThat(outcome.partition(TransactionReadSlot.id(TransactionReadSlot.Kind.USER, 0))).isNotNull();
    }

    @Test
    public void shouldLeaveStateUntouchedWhenConditionIsFalse()
    {
        TableMetadata table = table();
        ScalarTransactionProvider provider = new ScalarTransactionProvider();
        provider.seed(table, 1, 4);

        TransactionPlan plan = plan("BEGIN TRANSACTION\n" +
                                    "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1);\n" +
                                    "  IF source.v = 9 THEN\n" +
                                    "    UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 2;\n" +
                                    "  END IF\n" +
                                    "COMMIT TRANSACTION;");

        provider.execute(plan, context());

        Assertions.assertThat(provider.value(table, 2)).isNull();
        Assertions.assertThat(provider.value(table, 1)).isEqualTo(4);
    }

    @Test
    public void shouldDistinguishInequalityAndWholeRowPresenceConditions()
    {
        TableMetadata table = table();
        ScalarTransactionProvider provider = new ScalarTransactionProvider();
        provider.seed(table, 1, 4);

        TransactionPlan plan = plan("BEGIN TRANSACTION\n" +
                                    "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1);\n" +
                                    "  IF source.v != 9 AND source IS NOT NULL THEN\n" +
                                    "    UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 8 WHERE pk = 2;\n" +
                                    "    UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 6 WHERE pk = 3;\n" +
                                    "  END IF\n" +
                                    "COMMIT TRANSACTION;");

        provider.execute(plan, context());

        Assertions.assertThat(provider.value(table, 2)).isEqualTo(8);
        Assertions.assertThat(provider.value(table, 3)).isEqualTo(6);

        ScalarTransactionProvider empty = new ScalarTransactionProvider();
        TransactionPlan absent = plan("BEGIN TRANSACTION\n" +
                                      "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 9);\n" +
                                      "  IF source IS NULL THEN\n" +
                                      "    UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 5 WHERE pk = 4;\n" +
                                      "  END IF\n" +
                                      "COMMIT TRANSACTION;");
        empty.execute(absent, context());
        Assertions.assertThat(empty.value(table, 4)).isEqualTo(5);
    }

    @Test
    public void shouldResolveAReadReferenceAndPreserveMissingRows()
    {
        TableMetadata table = table();
        ScalarTransactionProvider provider = new ScalarTransactionProvider();
        provider.seed(table, 1, 13);

        TransactionPlan plan = plan("BEGIN TRANSACTION\n" +
                                    "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1);\n" +
                                    "  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = source.v WHERE pk = 2;\n" +
                                    "COMMIT TRANSACTION;");

        provider.execute(plan, context());

        Assertions.assertThat(provider.value(table, 2)).isEqualTo(13);

        ScalarTransactionProvider empty = new ScalarTransactionProvider();
        TransactionPlan missing = plan("BEGIN TRANSACTION\n" +
                                       "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 9);\n" +
                                       "  SELECT source.v;\n" +
                                       "COMMIT TRANSACTION;");
        FilteredPartition partition = empty.execute(missing, context())
                                             .partition(TransactionReadSlot.id(TransactionReadSlot.Kind.USER, 0));
        Assertions.assertThat(partition).isNotNull();
        Assertions.assertThat(partition.rowCount()).isZero();
    }

    @Test
    public void shouldRejectAnUnsupportedLaterWriteBeforeApplyingAnEarlierWrite()
    {
        TableMetadata table = table();
        ScalarTransactionProvider provider = new ScalarTransactionProvider();

        TransactionPlan plan = plan("BEGIN TRANSACTION\n" +
                                    "  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 2 WHERE pk = 1;\n" +
                                    "  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = v + 1 WHERE pk = 2;\n" +
                                    "COMMIT TRANSACTION;");

        Assertions.assertThatThrownBy(() -> provider.execute(plan, context()))
                  .isInstanceOf(org.apache.cassandra.exceptions.InvalidRequestException.class)
                  .hasMessageContaining("whole-column setters");
        Assertions.assertThat(provider.value(table, 1)).isNull();
        Assertions.assertThat(provider.value(table, 2)).isNull();
    }

    @Test
    public void shouldRejectNullComparisonLiteralBeforeExecution()
    {
        TableMetadata table = table();
        ScalarTransactionProvider provider = new ScalarTransactionProvider();
        provider.seed(table, 1, 4);
        TransactionPlan valid = plan("BEGIN TRANSACTION\n" +
                                     "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1);\n" +
                                     "  IF source.v = 4 THEN\n" +
                                     "    UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 2;\n" +
                                     "  END IF\n" +
                                     "COMMIT TRANSACTION;");
        TransactionCondition condition = valid.conditions().get(0);
        TransactionPlan invalid = new TransactionPlan(valid.reads(),
                                                       valid.writes(),
                                                       java.util.Collections.singletonList(new TransactionCondition(condition.kind(), condition.reference(), null)),
                                                       valid.tables(),
                                                       valid.returning(),
                                                       valid.minEpoch(),
                                                       valid.isNoOp());

        Assertions.assertThatThrownBy(() -> provider.execute(invalid, context()))
                  .isInstanceOf(org.apache.cassandra.exceptions.InvalidRequestException.class)
                  .hasMessageContaining("null or UNSET comparison literals");
        Assertions.assertThat(provider.value(table, 2)).isNull();
    }

    @Test
    public void shouldRejectAbsentWriteReferenceWithoutPublishingEarlierWrites()
    {
        TableMetadata table = table();
        ScalarTransactionProvider provider = new ScalarTransactionProvider();
        TransactionPlan plan = plan("BEGIN TRANSACTION\n" +
                                    "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 9);\n" +
                                    "  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 1 WHERE pk = 1;\n" +
                                    "  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = source.v WHERE pk = 2;\n" +
                                    "COMMIT TRANSACTION;");

        Assertions.assertThatThrownBy(() -> provider.execute(plan, context()))
                  .isInstanceOf(org.apache.cassandra.exceptions.InvalidRequestException.class)
                  .hasMessageContaining("null or absent write references");
        Assertions.assertThat(provider.value(table, 1)).isNull();
        Assertions.assertThat(provider.value(table, 2)).isNull();
    }

    private static TableMetadata table()
    {
        return Schema.instance.getTableMetadata(KEYSPACE, TABLE);
    }

    private static TransactionPlan plan(String query)
    {
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        CQLStatement statement = parsed.prepare(ClientState.forInternalCalls());
        TransactionStatement transaction = (TransactionStatement) statement;
        QueryOptions options = QueryProcessor.makeInternalOptions(transaction, new Object[0]);
        return transaction.toPlan(ClientState.forInternalCalls(), options);
    }

    private static TransactionExecutionContext context()
    {
        return new TransactionExecutionContext(new TransactionDomain("scalar-test", ScalarTransactionProvider.ID),
                                               null,
                                               ConsistencyLevel.SERIAL,
                                               ProtocolVersion.CURRENT,
                                               Dispatcher.RequestTime.forImmediateExecution());
    }
}
