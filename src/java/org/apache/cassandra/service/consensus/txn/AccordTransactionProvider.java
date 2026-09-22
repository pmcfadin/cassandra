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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import accord.primitives.Txn;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.txn.TxnDataKeyValue;
import org.apache.cassandra.service.accord.txn.TxnDataResult;
import org.apache.cassandra.service.accord.txn.TxnDataValue;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnValidationRejection;

import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.retry_new_protocol;
import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability;

/** The default transaction provider backed by Cassandra's existing Accord implementation. */
public final class AccordTransactionProvider implements TransactionProvider
{
    public static final String ID = "accord";
    public static final AccordTransactionProvider INSTANCE = new AccordTransactionProvider();
    private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(EnumSet.allOf(Capability.class));

    private AccordTransactionProvider()
    {
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

    @Override
    public TransactionOutcome execute(TransactionPlan plan, TransactionExecutionContext context)
    {
        Txn txn = AccordTransactionCompiler.compile(plan, context);
        if (txn == null)
            return TransactionOutcome.noOp();

        ConsistencyLevel consistency = context.consistency();
        if (consistency == null)
            throw new InvalidRequestException("Accord transaction execution requires an ordinary consistency level");

        TxnResult txnResult = AccordService.instance().coordinate(plan.minEpoch(), txn, consistency, context.requestTime());
        if (txnResult.kind() == retry_new_protocol)
            throw new InvalidRequestException("Transaction Statement is unsupported when migrating away from Accord or before migration to Accord is complete for a range");
        TxnValidationRejection.maybeThrow(txnResult);

        if (!(txnResult instanceof TxnDataResult))
            throw new IllegalStateException("Expected Accord transaction data result, got " + txnResult.kind());

        TxnDataResult data = (TxnDataResult) txnResult;
        Map<Integer, FilteredPartition> partitions = new HashMap<>();
        for (Map.Entry<Integer, TxnDataValue> entry : data.entrySet())
        {
            if (!(entry.getValue() instanceof TxnDataKeyValue))
                throw new InvalidRequestException("Accord transaction returned a range result for read slot " + entry.getKey());

            TxnDataKeyValue value = (TxnDataKeyValue) entry.getValue();
            try (RowIterator rows = value.rowIterator(false))
            {
                partitions.put(entry.getKey(), FilteredPartition.create(rows));
            }
        }
        return new TransactionOutcome(data.atMicros, partitions);
    }
}
