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

package org.apache.cassandra.io.sstable.representation;

import java.util.Collections;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.RepresentationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Creates the writers of the representation selected by a table's {@code representation} option.
 */
public final class SSTableRepresentations
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableRepresentations.class);

    private SSTableRepresentations()
    {
    }

    /**
     * Create the writers of the representation configured for the table of the given new sstable.
     *
     * @param operation the operation writing the sstable, or null if unknown
     */
    public static RepresentationWriters newWriters(Descriptor descriptor, TableMetadata metadata, SerializationHeader header, @Nullable OperationType operation)
    {
        RepresentationParams params = metadata.params.representation;
        if (!params.isEnabled() || SchemaConstants.isSystemKeyspace(metadata.keyspace))
            return RepresentationWriters.NONE;

        SSTableRepresentation.Writer writer;
        try
        {
            SSTableRepresentation representation = params.representation();
            writer = representation == null ? null : representation.newWriter(descriptor, metadata, header, operation);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            if (params.required)
                throw t;
            logger.warn("Failed to create sstable representation {} for {}; writing the sstable without it", params.className, descriptor, t);
            return RepresentationWriters.NONE;
        }

        if (writer == null)
            return RepresentationWriters.NONE;

        return new RepresentationWriters(descriptor, Collections.singletonList(new RepresentationWriters.Entry(params.className, params.required, writer)));
    }
}
