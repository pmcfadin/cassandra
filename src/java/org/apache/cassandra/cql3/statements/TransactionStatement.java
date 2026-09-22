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

package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.FunctionContext;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.transactions.ConditionStatement;
import org.apache.cassandra.cql3.transactions.ReferenceOperation;
import org.apache.cassandra.cql3.transactions.RowDataReference;
import org.apache.cassandra.cql3.transactions.SelectReferenceSource;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.consensus.txn.TransactionCondition;
import org.apache.cassandra.service.consensus.txn.TransactionDomainGuard;
import org.apache.cassandra.service.consensus.txn.TransactionExecutionContext;
import org.apache.cassandra.service.consensus.txn.TransactionOperation;
import org.apache.cassandra.service.consensus.txn.TransactionOutcome;
import org.apache.cassandra.service.consensus.txn.TransactionPlan;
import org.apache.cassandra.service.consensus.txn.TransactionProvider;
import org.apache.cassandra.service.consensus.txn.TransactionProviderRegistry;
import org.apache.cassandra.service.consensus.txn.TransactionProviders;
import org.apache.cassandra.service.consensus.txn.TransactionReference;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.service.consensus.txn.TransactionReadSlot.Kind.AUTO_READ;
import static org.apache.cassandra.service.consensus.txn.TransactionReadSlot.Kind.RETURNING;
import static org.apache.cassandra.service.consensus.txn.TransactionReadSlot.Kind.USER;
import static org.apache.cassandra.service.consensus.txn.TransactionReadSlot.id;

public class TransactionStatement implements CQLStatement.CompositeCQLStatement, CQLStatement.ReturningCQLStatement
{
    public static final String DUPLICATE_TUPLE_NAME_MESSAGE = "The name '%s' has already been used by a LET assignment.";
    public static final String INCOMPLETE_PARTITION_KEY_SELECT_MESSAGE = "SELECT must specify either all partition key elements. Partition key elements must be always specified with equality operators; %s %s";
    public static final String INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE = "SELECT must specify either all primary key elements or all partition key elements and LIMIT 1. In both cases partition key elements must be always specified with equality operators; %s %s";
    public static final String NO_CONDITIONS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify their own conditions; %s statement %s";
    public static final String NO_TIMESTAMPS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify custom timestamps; %s statement %s";
    public static final String NO_TTLS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify custom ttls; %s statement %s";
    public static final String TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE = "Accord transactions are disabled on table (See transactional_mode in table options); %s statement %s";
    public static final String TRANSACTIONS_DISABLED_ON_TABLE_BEING_DROPPED_MESSAGE = "Accord transactions are disabled on table (table is being dropped); %s statement %s";
    public static final String NO_COUNTERS_IN_TXNS_MESSAGE = "Counter columns cannot be accessed within a transaction; %s statement %s";
    public static final String NO_AGGREGATION_IN_TXNS_MESSAGE = "No aggregation functions allowed within a transaction; %s statement %s";
    public static final String NO_ORDER_BY_IN_TXNS_MESSAGE = "No ORDER BY clause allowed within a transaction; %s statement %s";
    public static final String NO_GROUP_BY_IN_TXNS_MESSAGE = "No GROUP BY clause allowed within a transaction; %s statement %s";
    public static final String EMPTY_TRANSACTION_MESSAGE = "Transaction contains no reads or writes";
    public static final String SELECT_REFS_NEED_COLUMN_MESSAGE = "SELECT references must specify a column.";
    public static final String TRANSACTIONS_DISABLED_MESSAGE = "Accord transactions are disabled. (See accord.enabled in cassandra.yaml)";
    public static final String ILLEGAL_RANGE_QUERY_MESSAGE = "Range queries are not allowed for reads within a transaction; %s %s";
    public static final String DUPLICATE_KEYS_IN_SAME_TRANSACTION_MESSAGE = "Transaction contains multiple updates to the same key and fields";
    public static final String UNSUPPORTED_MIGRATION = "Transaction Statement is unsupported when migrating away from Accord or before migration to Accord is complete for a range";
    public static final String NO_PARTITION_IN_CLAUSE_WITH_LIMIT = "Partition key is present in IN clause and there is a LIMIT... this is currently not supported; %s statement %s";
    public static final String WRITE_TXN_EMPTY_WITH_IGNORED_READS = "Write txn produced no mutation, and its reads do not return to the caller; ignoring...";
    public static final String WRITE_TXN_EMPTY_WITH_NO_READS = "Write txn produced no mutation, and had no reads; ignoring...";

    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(LoggerFactory.getLogger(TransactionStatement.class), 1, TimeUnit.MINUTES);

    static class NamedSelect
    {
        final int name;
        final SelectStatement select;

        public NamedSelect(int name, SelectStatement select)
        {
            this.name = name;
            this.select = select;
        }
    }

    private final List<NamedSelect> assignments;
    private final NamedSelect returningSelect;
    private final List<RowDataReference> returningReferences;
    private final List<ModificationStatement> updates;
    private final List<ConditionStatement> conditions;

    private final VariableSpecifications bindVariables;
    private final ResultSet.ResultMetadata resultMetadata;

    public TransactionStatement(List<NamedSelect> assignments,
                                NamedSelect returningSelect,
                                List<RowDataReference> returningReferences,
                                List<ModificationStatement> updates,
                                List<ConditionStatement> conditions,
                                VariableSpecifications bindVariables)
    {
        this.assignments = assignments;
        this.returningSelect = returningSelect;
        this.returningReferences = returningReferences;
        this.updates = updates;
        this.conditions = conditions;
        this.bindVariables = bindVariables;

        if (returningSelect != null)
        {
            resultMetadata = returningSelect.select.getResultMetadata();
        }
        else if (returningReferences != null && !returningReferences.isEmpty())
        {
            List<ColumnSpecification> names = new ArrayList<>(returningReferences.size());
            for (RowDataReference reference : returningReferences)
                names.add(reference.toResultMetadata());
            resultMetadata = new ResultSet.ResultMetadata(names);
        }
        else
        {
            resultMetadata =  ResultSet.ResultMetadata.EMPTY;
        }
    }

    public List<ModificationStatement> getUpdates()
    {
        return updates;
    }

    @Override
    public ImmutableList<ColumnSpecification> getBindVariables()
    {
        return bindVariables.getImmutableBindVariables();
    }

    @Override
    public short[] getPartitionKeyBindVariableIndexes()
    {
        if (returningSelect != null)
        {
            short[] result = returningSelect.select.getPartitionKeyBindVariableIndexes();
            if (result != null)
                return result;
        }

        for (ModificationStatement stmt : updates)
        {
            short[] result = stmt.getPartitionKeyBindVariableIndexes();
            if (result != null)
                return result;
        }
        return null;
    }

    @Override
    public void authorize(ClientState state)
    {
        // Assess read permissions for all data from both explicit LET statements and generated reads.
        for (NamedSelect let : assignments)
            let.select.authorize(state);

        if (returningSelect != null)
            returningSelect.select.authorize(state);

        ModificationStatement.TableAuthorizationState authorizationState = new ModificationStatement.TableAuthorizationState();
        for (ModificationStatement update : updates)
            update.authorize(state, authorizationState);
    }

    @Override
    public void validate(ClientState state)
    {
        for (NamedSelect statement : assignments)
            statement.select.validate(state);
        if (returningSelect != null)
            returningSelect.select.validate(state);
        for (ModificationStatement statement : updates)
            statement.validate(state);
    }

    @Override
    public Iterable<CQLStatement> getStatements()
    {
        return () -> {
            Stream<CQLStatement> stream = assignments.stream().map(n -> n.select);
            if (returningSelect != null)
                stream = Stream.concat(stream, Stream.of(returningSelect.select));
            stream = Stream.concat(stream, updates.stream());
            return stream.iterator();
        };
    }

    @Override
    public ResultSet.ResultMetadata getResultMetadata()
    {
        return resultMetadata;
    }

    private Map<TableId, TableMetadata> collectTableMetadata()
    {
        Map<TableId, TableMetadata> tables = new LinkedHashMap<>();
        if (updates != null)
        {
            for (ModificationStatement modification : updates)
                tables.put(modification.metadata.id, modification.metadata);
        }
        if (assignments != null)
        {
            for (NamedSelect select : assignments)
                tables.put(select.select.table.id, select.select.table);
        }
        if (returningSelect != null)
            tables.put(returningSelect.select.table.id, returningSelect.select.table);
        if (returningReferences != null)
        {
            for (RowDataReference ref : returningReferences)
                if (ref.table() != null)
                    tables.put(ref.table().id, ref.table());
        }
        return tables;
    }

    /**
     * Returns all tables referenced by this transaction, including tables read by LET assignments,
     * returned SELECTs, and returned references.
     */
    public Set<TableId> referencedTableIds()
    {
        return Collections.unmodifiableSet(new HashSet<>(collectTableMetadata().keySet()));
    }

    /**
     * Returns the capabilities required by this transaction for provider admission.
     *
     * Writes conservatively require READ as Accord may generate reads while compiling a mutation.
     */
    public Set<TransactionProvider.Capability> requiredCapabilities()
    {
        Set<TransactionProvider.Capability> required = EnumSet.of(TransactionProvider.Capability.STRICT_SERIALIZABLE);

        if (!updates.isEmpty())
        {
            required.add(TransactionProvider.Capability.WRITE);
            required.add(TransactionProvider.Capability.READ);
        }
        if (!assignments.isEmpty() || returningSelect != null || (returningReferences != null && !returningReferences.isEmpty()))
            required.add(TransactionProvider.Capability.READ);
        if (!conditions.isEmpty())
            required.add(TransactionProvider.Capability.CONDITIONAL);
        if (referencedTableIds().size() > 1)
            required.add(TransactionProvider.Capability.MULTI_TABLE);

        return Collections.unmodifiableSet(required);
    }

    /**
     * Returns {@code true} only if the statement selects multiple clusterings in a partition
     */
    private static boolean isSelectingMultipleClusterings(SelectStatement select, @Nullable QueryOptions options)
    {
        if (select.getRestrictions().hasAllPrimaryKeyColumnsRestrictedByEqualities())
            return false;

        if (options == null)
        {
            // if the limit is a non-terminal marker (because we're preparing), defer validation until execution (when options != null)
            if (select.isLimitMarker())
                return false;

            options = QueryOptions.DEFAULT;
        }

        return select.getLimit(options) != 1;
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime)
    {
        Set<TableId> tableIds = referencedTableIds();
        TransactionDomainGuard.checkTransactionTables(tableIds, "TRANSACTION");
        if (!TransactionDomainGuard.hasActiveExternal(tableIds))
            checkTrue(DatabaseDescriptor.getAccordTransactionsEnabled(), TRANSACTIONS_DISABLED_MESSAGE);

        // check again since now we have query options; note that statements are quaranted to be single partition reads at this point
        for (NamedSelect assignment : assignments)
        {
            checkFalse(isSelectingMultipleClusterings(assignment.select, options), INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, "LET assignment", assignment.select.source);
            if (assignment.select.getRestrictions().keyIsInRelation())
                checkTrue(assignment.select.getLimit(options) == DataLimits.NO_LIMIT, NO_PARTITION_IN_CLAUSE_WITH_LIMIT, "SELECT", assignment.select.source);
        }
        if (returningSelect != null && returningSelect.select.getRestrictions().keyIsInRelation())
        {
            checkTrue(returningSelect.select.getLimit(options) == DataLimits.NO_LIMIT, NO_PARTITION_IN_CLAUSE_WITH_LIMIT, "SELECT", returningSelect.select.source);
        }

        TransactionProviderRegistry.Selection selection = TransactionProviders.registry().select(tableIds, requiredCapabilities());
        TransactionExecutionContext context = new TransactionExecutionContext(selection.domain(),
                                                                                options.getConsistency(),
                                                                                options.getSerialConsistency(),
                                                                                options.getProtocolVersion(),
                                                                                requestTime);
        TransactionPlan plan = toPlan(state.getClientState(), options);
        return renderOutcome(plan, selection.provider().execute(plan, context), options);
    }

    public TransactionPlan toPlan(ClientState state, QueryOptions options)
    {
        for (NamedSelect assignment : assignments)
            validateExternalSelection(assignment.select, options);
        if (returningSelect != null)
            validateExternalSelection(returningSelect.select, options);
        Map<Integer, NamedSelect> autoReads = new LinkedHashMap<>();
        List<TransactionPlan.Write> planWrites = new ArrayList<>();
        Map<Object, Columns> seenColumns = new HashMap<>();
        if (!updates.isEmpty())
        {
            for (int index = 0; index < updates.size(); index++)
            {
                ModificationStatement modification = updates.get(index);
                List<PartitionUpdate> baseUpdates = modification.getTxnUpdate(state, options);
                List<Clustering<?>> clusterings = modification.txnClusterings(options, state);
                List<TransactionOperation> regularOperations = modification.getTxnRegularOperations(options);
                List<TransactionOperation> staticOperations = modification.getTxnStaticOperations(options);
                Columns regularColumns = modification.updatedColumns().columns(false);
                Columns staticColumns = modification.updatedColumns().columns(true);
                for (PartitionUpdate baseUpdate : baseUpdates)
                {
                    DecoratedKey key = baseUpdate.partitionKey();
                    for (Row row : baseUpdate)
                        seenColumns.merge(new ModificationStatement.RowKey(key, row.clustering()), regularColumns, TransactionStatement::mergeColumnsIfNoDuplicates);
                    seenColumns.merge(key, staticColumns, TransactionStatement::mergeColumnsIfNoDuplicates);
                    planWrites.add(new TransactionPlan.Write(index,
                                                             baseUpdate,
                                                             clusterings,
                                                             regularOperations,
                                                             staticOperations));
                }
                if (modification.allReferenceOperations().stream().anyMatch(ReferenceOperation::requiresRead))
                {
                    int slot = id(AUTO_READ, index);
                    if (!autoReads.containsKey(slot))
                        autoReads.put(slot, new NamedSelect(slot, modification.createSelectForTxn()));
                }
            }
        }

        List<TransactionPlan.Read> planReads = new ArrayList<>();
        for (NamedSelect assignment : assignments)
            addPlanReads(planReads, assignment, options, true);

        List<Integer> returningSlots = new ArrayList<>();
        if (returningSelect != null)
        {
            int firstReturningRead = planReads.size();
            addPlanReads(planReads, returningSelect, options, false);
            for (int i = firstReturningRead; i < planReads.size(); i++)
                returningSlots.add(planReads.get(i).slot());
        }

        if (!planWrites.isEmpty())
        {
            for (NamedSelect autoRead : autoReads.values())
                addPlanReads(planReads, autoRead, options, true);
        }

        TransactionPlan.ReturnSelection returning = TransactionPlan.ReturnSelection.none();
        if (returningSelect != null)
        {
            returning = TransactionPlan.ReturnSelection.rows(returningSlots);
        }
        else if (returningReferences != null)
        {
            List<TransactionReference> references = new ArrayList<>(returningReferences.size());
            for (RowDataReference reference : returningReferences)
                references.add(reference.toTransactionReference(options));
            returning = TransactionPlan.ReturnSelection.references(references);
        }

        List<TransactionCondition> planConditions = new ArrayList<>();
        if (!planWrites.isEmpty())
            for (ConditionStatement condition : conditions)
                planConditions.add(condition.toTransactionCondition(options));

        Map<TableId, TableMetadata> tableMap = collectTableMetadata();
        long minEpoch = Epoch.EMPTY.getEpoch();
        for (TableMetadata table : tableMap.values())
            minEpoch = Math.max(minEpoch, table.epoch.getEpoch());

        boolean noOp = planWrites.isEmpty() && (returning.kind() == TransactionPlan.ReturnSelection.Kind.NONE || planReads.isEmpty());
        if (noOp)
        {
            noSpamLogger.info(planReads.isEmpty() ? WRITE_TXN_EMPTY_WITH_NO_READS : WRITE_TXN_EMPTY_WITH_IGNORED_READS);
            planReads.clear();
        }
        return new TransactionPlan(planReads, planWrites, planConditions, tableMap, returning, minEpoch, noOp);
    }

    private static Columns mergeColumnsIfNoDuplicates(Columns existing, Columns add)
    {
        Columns merged = existing.mergeTo(add);
        if (merged.size() != existing.size() + add.size())
            throw invalidRequest(DUPLICATE_KEYS_IN_SAME_TRANSACTION_MESSAGE);
        return merged;
    }

    private void addPlanReads(List<TransactionPlan.Read> result, NamedSelect namedSelect, QueryOptions options, boolean requireSingle)
    {
        @SuppressWarnings("unchecked")
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> query =
        (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) namedSelect.select.getQuery(options, 0);
        if (requireSingle && query.queries.size() != 1)
            throw invalidRequest("Within a transaction, SELECT statements must select a single partition; found " + query.queries.size() + " partitions");
        if (query.queries.size() == 1)
        {
            result.add(new TransactionPlan.Read(namedSelect.name, query.queries.get(0), namedSelect.select.getSelection().getColumns()));
            return;
        }
        for (int i = 0; i < query.queries.size(); i++)
            result.add(new TransactionPlan.Read(id(RETURNING, i), query.queries.get(i), namedSelect.select.getSelection().getColumns()));
    }

    private ResultMessage renderOutcome(TransactionPlan plan, TransactionOutcome outcome, QueryOptions options)
    {
        if (plan.isNoOp() || outcome.isNoOp())
            return new ResultMessage.Void();

        if (returningSelect != null)
        {
            Map<Integer, SinglePartitionReadCommand> commands = new HashMap<>();
            for (TransactionPlan.Read read : plan.reads())
                commands.put(read.slot(), read.command());
            Selection.Selectors selectors = returningSelect.select.getSelection().newSelectors(options);
            long atMicros = outcome.atMicros();
            FunctionContext context = new FunctionContext.MicrosFunctionContext(atMicros)
            {
                @Override public QueryOptions options() { return options; }
            };
            ResultSetBuilder result = new ResultSetBuilder(resultMetadata, context, selectors, false);
            long atSeconds = atMicros / 1000_000;
            for (int slot : plan.returning().slots())
            {
                FilteredPartition partition = outcome.partition(slot);
                if (partition != null)
                {
                    boolean reversed = commands.get(slot).isReversed();
                    try (RowIterator rows = partition.rowIterator(reversed))
                    {
                        returningSelect.select.processPartition(rows, options, result, atSeconds);
                    }
                }
            }
            return new ResultMessage.Rows(result.build());
        }

        if (returningReferences != null)
        {
            List<AbstractType<?>> resultType = new ArrayList<>(returningReferences.size());
            List<ColumnMetadata> columns = new ArrayList<>(returningReferences.size());
            for (RowDataReference reference : returningReferences)
            {
                ColumnMetadata forMetadata = reference.toResultMetadata();
                resultType.add(forMetadata.type);
                columns.add(reference.column());
            }

            ResultSetBuilder result = new ResultSetBuilder(resultMetadata, FunctionContext.NONE, Selection.noopSelector(), false);
            result.newRow(options.getProtocolVersion(), null, null, columns);
            List<TransactionReference> references = plan.returning().references();
            for (int i = 0; i < references.size(); i++)
                result.add(references.get(i).toByteBuffer(outcome.partition(references.get(i).slot()), resultType.get(i)));
            return new ResultMessage.Rows(result.build());
        }

        return new ResultMessage.Void();
    }

    @Override
    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        return execute(state, options, Dispatcher.RequestTime.forImmediateExecution());
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.TRANSACTION);
    }

    @Override
    public boolean eligibleAsPreparedStatement()
    {
        // false is the default, but still best to be explicit.
        return false;
    }

    private static void validate(SelectStatement.RawStatement select)
    {
        if (select.parameters.orderings != null && !select.parameters.orderings.isEmpty())
            throw invalidRequest(NO_ORDER_BY_IN_TXNS_MESSAGE, "SELECT", select.source);
        if (select.parameters.groups != null && !select.parameters.groups.isEmpty())
            throw invalidRequest(NO_GROUP_BY_IN_TXNS_MESSAGE, "SELECT", select.source);
    }

    private static void validateExternalSelection(SelectStatement select, QueryOptions options)
    {
        if (TransactionDomainGuard.isActiveExternal(select.table))
        {
            // The experimental scalar profile stores values, without Cassandra cell metadata.
            org.apache.cassandra.cql3.selection.Selection.Selectors selectors = select.getSelection().newSelectors(options);
            checkFalse(selectors.hasProcessing() || selectors.collectWritetimes() || selectors.collectTTLs(),
                       "External scalar transactions support only direct column selections");
        }
    }

    private static void validate(SelectStatement prepared)
    {
        TransactionDomainGuard.checkTransaction(prepared.table, "TRANSACTION");
        if (!TransactionDomainGuard.isActiveExternal(prepared.table) && !prepared.table.isAccordEnabled())
            throw invalidRequest(TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE, "SELECT", prepared.source);
        if (prepared.table.params.pendingDrop)
            throw invalidRequest(TRANSACTIONS_DISABLED_ON_TABLE_BEING_DROPPED_MESSAGE, "SELECT", prepared.source);
        if (prepared.table.isCounter())
            throw invalidRequest(NO_COUNTERS_IN_TXNS_MESSAGE, "SELECT", prepared.source);
        if (prepared.hasAggregation())
            throw invalidRequest(NO_AGGREGATION_IN_TXNS_MESSAGE, "SELECT", prepared.source);

        // when "LIMIT ?" this check can't be performed, so need to do again once the options are known
        if (prepared.getRestrictions().keyIsInRelation())
            checkTrue(prepared.isLimitMarker() || prepared.getLimit(null) == DataLimits.NO_LIMIT, NO_PARTITION_IN_CLAUSE_WITH_LIMIT, "SELECT", prepared.source);
    }

    public static class Parsed extends QualifiedStatement.Composite
    {
        private final List<SelectStatement.RawStatement> assignments;
        private final SelectStatement.RawStatement select;
        private final List<RowDataReference.Raw> returning;
        private final List<ModificationStatement.Parsed> updates;
        private final List<ConditionStatement.Raw> conditions;
        private final List<RowDataReference.Raw> dataReferences;

        public Parsed(List<SelectStatement.RawStatement> assignments,
                      SelectStatement.RawStatement select,
                      List<RowDataReference.Raw> returning,
                      List<ModificationStatement.Parsed> updates,
                      List<ConditionStatement.Raw> conditions,
                      List<RowDataReference.Raw> dataReferences)
        {
            this.assignments = assignments;
            this.select = select;
            this.returning = returning;
            this.updates = updates;
            this.conditions = conditions != null ? conditions : Collections.emptyList();
            this.dataReferences = dataReferences;
        }

        @Override
        protected Iterable<? extends QualifiedStatement> getStatements()
        {
            Iterable<QualifiedStatement> group = Iterables.concat(assignments, updates);
            if (select != null)
                group = Iterables.concat(group, Collections.singleton(select));
            return group;
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            checkFalse(updates.isEmpty() && returning == null && select == null, EMPTY_TRANSACTION_MESSAGE);

            if (select != null || returning != null)
                checkTrue(select != null ^ returning != null, "Cannot specify both a full SELECT and a SELECT w/ LET references.");

            bindVariables.setSaveTargetOwners(true);
            List<NamedSelect> preparedAssignments = new ArrayList<>(assignments.size());
            Map<Integer, RowDataReference.ReferenceSource> refSources = new HashMap<>();
            Set<String> selectNames = new HashSet<>();

            int userReadIndex = 0;
            Map<String, Integer> nameToTxnDataName = new HashMap<>();
            for (SelectStatement.RawStatement select : assignments)
            {
                checkNotNull(select.parameters.refName, "Assignments must be named");
                int name = id(USER, userReadIndex++);
                nameToTxnDataName.put(select.parameters.refName, name);
                checkTrue(selectNames.add(select.parameters.refName), DUPLICATE_TUPLE_NAME_MESSAGE, select.parameters.refName);
                validate(select);

                SelectStatement prepared = select.prepare(bindVariables);
                validate(prepared);

                NamedSelect namedSelect = new NamedSelect(name, prepared);
                checkAtMostOneRowSpecified(namedSelect.select, "LET assignment " + select.parameters.refName);
                preparedAssignments.add(namedSelect);
                refSources.put(name, new SelectReferenceSource(prepared));
            }

            if (dataReferences != null)
                for (RowDataReference.Raw reference : dataReferences)
                    reference.resolveReference(refSources, nameToTxnDataName, userReadIndex++);

            NamedSelect returningSelect = null;
            if (select != null)
            {
                validate(select);
                SelectStatement prepared = select.prepare(bindVariables);
                validate(prepared);
                returningSelect = new NamedSelect(id(RETURNING), prepared);
                checkAtMostOnePartitionSpecified(returningSelect.select, "returning select");
            }

            List<RowDataReference> returningReferences = null;

            if (returning != null)
            {
                // TODO: Eliminate/modify this check if we allow full tuple selections.
                returningReferences = returning.stream().peek(raw -> checkTrue(raw.column() != null, SELECT_REFS_NEED_COLUMN_MESSAGE))
                                                        .map(RowDataReference.Raw::prepareAsReceiver)
                                                        .collect(Collectors.toList());
            }

            List<ModificationStatement> preparedUpdates = new ArrayList<>(updates.size());
            
            // check for any read-before-write updates
            for (int i = 0; i < updates.size(); i++)
            {
                ModificationStatement.Parsed parsed = updates.get(i);

                ModificationStatement prepared = parsed.prepare(state, bindVariables);
                TransactionDomainGuard.checkTransaction(prepared.metadata(), "TRANSACTION");
                checkTrue(TransactionDomainGuard.isActiveExternal(prepared.metadata()) || prepared.metadata().isAccordEnabled(),
                          TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE, prepared.type, prepared.source);
                checkFalse(prepared.metadata().params.pendingDrop, TRANSACTIONS_DISABLED_ON_TABLE_BEING_DROPPED_MESSAGE, prepared.type, prepared.source);
                checkFalse(prepared.hasConditions(), NO_CONDITIONS_IN_UPDATES_MESSAGE, prepared.type, prepared.source);
                checkFalse(prepared.isTimestampSet(), NO_TIMESTAMPS_IN_UPDATES_MESSAGE, prepared.type, prepared.source);
                checkFalse(prepared.attrs.isTimeToLiveSet(), NO_TTLS_IN_UPDATES_MESSAGE, prepared.type, prepared.source);

                if (prepared.metadata().isCounter())
                    throw invalidRequest(NO_COUNTERS_IN_TXNS_MESSAGE, prepared.type, prepared.source);

                preparedUpdates.add(prepared);
            }

            List<ConditionStatement> preparedConditions = new ArrayList<>(conditions.size());
            for (ConditionStatement.Raw condition : conditions)
                // TODO: If we eventually support IF ks.function(ref) THEN, the keyspace will have to be provided here
                preparedConditions.add(condition.prepare("[txn]", bindVariables));

            return new TransactionStatement(preparedAssignments, returningSelect, returningReferences, preparedUpdates, preparedConditions, bindVariables);
        }

        /**
         * Do not use this method in execution!!! It is only allowed during prepare because it outputs a query raw text.
         * We don't want it print it for a user who provided an identifier of someone's else prepared statement.
         */
        private static void checkAtMostOnePartitionSpecified(SelectStatement select, String name)
        {
            checkTrue(select.getRestrictions().hasPartitionKeyRestrictions(), INCOMPLETE_PARTITION_KEY_SELECT_MESSAGE, name, select.source);
        }

        /**
         * Do not use this method in execution!!! It is only allowed during prepare because it outputs a query raw text.
         * We don't want it print it for a user who provided an identifier of someone's else prepared statement.
         */
        private static void checkAtMostOneRowSpecified(SelectStatement select, String name)
        {
            checkFalse(select.isPartitionRangeQuery(), ILLEGAL_RANGE_QUERY_MESSAGE, name, select.source);
            checkFalse(isSelectingMultipleClusterings(select, null), INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, name, select.source);
        }
    }
}
