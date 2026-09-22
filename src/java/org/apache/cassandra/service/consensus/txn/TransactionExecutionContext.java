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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Immutable request facts supplied after transaction domain admission.
 *
 * The ordinary consistency field is nullable where existing internal transaction
 * helpers permit a missing level. Serial consistency is always resolved.
 */
public final class TransactionExecutionContext
{
    private final TransactionDomain domain;
    private final ConsistencyLevel consistency;
    private final ConsistencyLevel serialConsistency;
    private final ProtocolVersion protocolVersion;
    private final Dispatcher.RequestTime requestTime;

    public TransactionExecutionContext(TransactionDomain domain,
                                       ConsistencyLevel consistency,
                                       ConsistencyLevel serialConsistency,
                                       ProtocolVersion protocolVersion,
                                       Dispatcher.RequestTime requestTime)
    {
        this.domain = Objects.requireNonNull(domain, "domain");
        this.consistency = consistency;
        this.serialConsistency = Objects.requireNonNull(serialConsistency, "serialConsistency");
        this.protocolVersion = Objects.requireNonNull(protocolVersion, "protocolVersion");
        this.requestTime = Objects.requireNonNull(requestTime, "requestTime");
    }

    public TransactionDomain domain()
    {
        return domain;
    }

    public ConsistencyLevel consistency()
    {
        return consistency;
    }

    public ConsistencyLevel serialConsistency()
    {
        return serialConsistency;
    }

    public ProtocolVersion protocolVersion()
    {
        return protocolVersion;
    }

    public Dispatcher.RequestTime requestTime()
    {
        return requestTime;
    }
}
