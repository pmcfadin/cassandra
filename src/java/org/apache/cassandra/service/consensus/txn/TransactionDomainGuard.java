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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Denial fence for experimental domains. PREPARED domains have no admitted data path yet.
 * The schema marker also closes local schema installation and metadata replay windows.
 */
public final class TransactionDomainGuard
{
    public static final String MARKER = "cassandra.experimental.transaction_domain";

    private TransactionDomainGuard()
    {
    }

    public static boolean isReserved(TableMetadata table)
    {
        return table != null && table.params.extensions.containsKey(MARKER);
    }

    /** Opaque maintenance payloads must be decoded if any local ownership fence exists. */
    public static boolean hasReservations()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata != null && !metadata.consistencyDomains.isEmpty())
            return true;
        for (KeyspaceMetadata keyspace : Schema.instance.distributedAndLocalKeyspaces())
            for (TableMetadata table : keyspace.tablesAndViews())
                if (isReserved(table))
                    return true;
        return false;
    }

    public static TableMetadata markReserved(TableMetadata table, UUID domainId)
    {
        if (isReserved(table))
            throw new InvalidRequestException("Table already has a transaction domain marker");
        Map<String, ByteBuffer> extensions = new HashMap<>(table.params.extensions);
        extensions.put(MARKER, ByteBufferUtil.bytes(domainId.toString()));
        return table.unbuild().extensions(extensions).build();
    }

    public static void check(TableMetadata table, String operation)
    {
        if (isReserved(table))
            reject(table.id, operation);
        check(table.id, operation);
    }

    /**
     * Admission used only by transaction preparation and execution.  The ordinary
     * guard deliberately remains a closed fence for every reserved table,
     * including ACTIVE external domains.
     */
    public static void checkTransaction(TableMetadata table, String operation)
    {
        if (table == null)
            throw new InvalidRequestException("Transaction table metadata is missing");

        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        TransactionDomainDescriptor descriptor = metadata == null ? null : metadata.consistencyDomains.forTable(table.id);
        if (!isReserved(table) && descriptor == null)
        {
            check(table, operation);
            return;
        }

        if (descriptor == null)
            reject(table.id, operation);
        if (descriptor.state() != TransactionDomainDescriptor.State.ACTIVE || descriptor.externalBinding() == null)
            reject(table.id, operation);
        if (!isReserved(table) || !markerMatches(table, descriptor))
            reject(table.id, operation);

        TableMetadata authoritative = metadata.schema.getKeyspaces().getTableOrViewNullable(table.id);
        if (authoritative == null || !authoritative.equals(table))
            reject(table.id, operation);
    }

    public static void checkTransaction(TableId tableId, String operation)
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        TableMetadata table = metadata == null ? Schema.instance.getTableMetadata(tableId)
                                               : metadata.schema.getKeyspaces().getTableOrViewNullable(tableId);
        if (table == null)
            throw new InvalidRequestException("Unknown transaction table: " + tableId);
        checkTransaction(table, operation);
    }

    public static void checkTransactionTables(Iterable<TableId> tables, String operation)
    {
        for (TableId table : tables)
            checkTransaction(table, operation);
    }

    public static boolean isActiveExternal(TableMetadata table)
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (table == null || metadata == null)
            return false;
        TransactionDomainDescriptor descriptor = metadata.consistencyDomains.forTable(table.id);
        return descriptor != null && descriptor.state() == TransactionDomainDescriptor.State.ACTIVE
               && descriptor.externalBinding() != null;
    }

    public static boolean hasActiveExternal(Iterable<TableId> tables)
    {
        for (TableId tableId : tables)
        {
            ClusterMetadata metadata = ClusterMetadata.currentNullable();
            TableMetadata table = metadata == null ? null : metadata.schema.getKeyspaces().getTableOrViewNullable(tableId);
            if (isActiveExternal(table))
                return true;
        }
        return false;
    }

    private static boolean markerMatches(TableMetadata table, TransactionDomainDescriptor descriptor)
    {
        ByteBuffer marker = table.params.extensions.get(MARKER);
        return marker != null && marker.equals(ByteBufferUtil.bytes(descriptor.id().toString()));
    }

    public static void check(TableId tableId, String operation)
    {
        if (isReserved(Schema.instance.getTableMetadata(tableId)))
            reject(tableId, operation);
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata != null && (metadata.consistencyDomains.forTable(tableId) != null ||
                                 isReserved(metadata.schema.getKeyspaces().getTableOrViewNullable(tableId))))
            reject(tableId, operation);
    }

    public static void check(Mutation mutation, String operation)
    {
        // Validate the entire mutation before any constituent update can reach a log or memtable.
        for (PartitionUpdate update : mutation.getPartitionUpdates())
            check(update.metadata(), operation);
    }

    public static void checkTables(Iterable<TableId> tables, String operation)
    {
        for (TableId table : tables)
            check(table, operation);
    }

    public static void checkKeyspace(String keyspace, String operation)
    {
        KeyspaceMetadata local = Schema.instance.getKeyspaceMetadata(keyspace);
        if (local != null)
            for (TableMetadata table : local.tablesAndViews())
                check(table, operation);
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata != null)
        {
            KeyspaceMetadata authoritative = metadata.schema.getKeyspaces().getNullable(keyspace);
            if (authoritative != null)
                for (TableMetadata table : authoritative.tablesAndViews())
                    check(table, operation);
        }
    }

    /** Pure metadata validation: generic DDL cannot create or change ownership fences. */
    public static void checkSchemaChange(ClusterMetadata before, Keyspaces after)
    {
        Keyspaces previous = before.schema.getKeyspaces();
        for (KeyspaceMetadata keyspace : previous)
        {
            KeyspaceMetadata replacement = after.getNullable(keyspace.name);
            for (TableMetadata table : keyspace.tablesAndViews())
            {
                if (!isReserved(table) && before.consistencyDomains.forTable(table.id) == null)
                    continue;
                TableMetadata next = after.getTableOrViewNullable(table.id);
                if (replacement == null || !keyspace.params.equals(replacement.params) || !table.equals(next))
                    reject(table.id, "schema change");
                for (ViewMetadata view : replacement.views)
                    if (view.baseTableId.equals(table.id))
                        reject(table.id, "materialized view creation");
            }
        }
        for (KeyspaceMetadata keyspace : after)
            for (TableMetadata table : keyspace.tablesAndViews())
                if (isReserved(table))
                {
                    TableMetadata prior = previous.getTableOrViewNullable(table.id);
                    if (!isReserved(prior) || !prior.equals(table))
                        reject(table.id, "schema marker change");
                }
    }

    private static void reject(TableId table, String operation)
    {
        String state = "PREPARED";
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata != null)
        {
            TransactionDomainDescriptor descriptor = metadata.consistencyDomains.forTable(table);
            if (descriptor != null && descriptor.state() == TransactionDomainDescriptor.State.ACTIVE)
                state = "ACTIVE";
        }
        throw new InvalidRequestException("Transaction domain for table " + table +
                                          " is " + state + " (closed); " + operation + " is not admitted");
    }
}
