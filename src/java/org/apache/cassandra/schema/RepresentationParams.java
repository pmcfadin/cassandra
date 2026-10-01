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

package org.apache.cassandra.schema;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.representation.SSTableRepresentation;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

/**
 * The {@code representation} table option: an additional {@link SSTableRepresentation} written with every sstable of
 * the table.
 * <p>
 * Only the option strings are part of the schema. The implementation class is resolved lazily on the node writing an
 * sstable, so that a node which does not have the implementation on its classpath can still load the schema; such a
 * node writes sstables without an optional representation, and fails to write sstables if it is required.
 */
public final class RepresentationParams
{
    private static final Logger logger = LoggerFactory.getLogger(RepresentationParams.class);

    public static final String CLASS = "class";
    public static final String REQUIRED = "required";

    private static final String DEFAULT_PACKAGE = "org.apache.cassandra.io.sstable.representation.";

    public static final RepresentationParams NONE = new RepresentationParams(null, false, ImmutableMap.of());

    @Nullable
    public final String className;
    public final boolean required;
    public final ImmutableMap<String, String> options;

    private volatile SSTableRepresentation representation;
    private volatile boolean unavailable;

    private RepresentationParams(@Nullable String className, boolean required, ImmutableMap<String, String> options)
    {
        this.className = className;
        this.required = required;
        this.options = options;
    }

    public static RepresentationParams fromMap(@Nullable Map<String, String> map)
    {
        if (map == null || map.isEmpty())
            return NONE;

        Map<String, String> options = new HashMap<>(map);
        String className = options.remove(CLASS);
        if (className == null || className.isEmpty())
            throw new ConfigurationException(format("Missing sub-option '%s' for the '%s' option", CLASS, TableParams.Option.REPRESENTATION));

        String required = options.remove(REQUIRED);
        if (required != null && !required.equalsIgnoreCase("true") && !required.equalsIgnoreCase("false"))
            throw new ConfigurationException(format("Invalid value '%s' for sub-option '%s' of the '%s' option: must be true or false",
                                                    required, REQUIRED, TableParams.Option.REPRESENTATION));

        return new RepresentationParams(className, Boolean.parseBoolean(required), ImmutableMap.copyOf(options));
    }

    public boolean isEnabled()
    {
        return className != null;
    }

    public Map<String, String> asMap()
    {
        if (!isEnabled())
            return ImmutableMap.of();

        return ImmutableMap.<String, String>builder()
                           .put(CLASS, className)
                           .put(REQUIRED, Boolean.toString(required))
                           .putAll(options)
                           .build();
    }

    /**
     * Check that the implementation exists on this node and accepts the options for the given table. Called on the
     * coordinator of a schema change only.
     */
    public void validate(TableMetadata metadata)
    {
        if (!isEnabled())
            return;

        if (SchemaConstants.isSystemKeyspace(metadata.keyspace))
            throw new ConfigurationException(format("The '%s' option cannot be set on system tables", TableParams.Option.REPRESENTATION));

        newInstance().validate(metadata);
    }

    /**
     * The implementation, or null if there is none or it cannot be created on this node and is not required.
     *
     * @throws ConfigurationException if a required implementation cannot be created
     */
    @Nullable
    public SSTableRepresentation representation()
    {
        if (!isEnabled())
            return null;

        SSTableRepresentation current = representation;
        if (current != null || (unavailable && !required))
            return current;

        synchronized (this)
        {
            if (representation == null)
            {
                try
                {
                    representation = newInstance();
                }
                catch (ConfigurationException e)
                {
                    if (required)
                        throw e;
                    if (!unavailable)
                        logger.warn("Sstable representation {} is not available on this node; sstables will be written without it", className, e);
                    unavailable = true;
                }
            }
            return representation;
        }
    }

    private SSTableRepresentation newInstance()
    {
        String name = className.contains(".") ? className : DEFAULT_PACKAGE + className;
        Class<? extends SSTableRepresentation> cls = FBUtilities.classForNameWithoutInitialization(name, "sstable representation", SSTableRepresentation.class);
        try
        {
            return cls.getConstructor(Map.class).newInstance(options);
        }
        catch (NoSuchMethodException e)
        {
            throw new ConfigurationException(format("Sstable representation %s must have a public constructor taking a Map<String, String> of options", name), e);
        }
        catch (InvocationTargetException e)
        {
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException) e.getCause();
            throw new ConfigurationException(format("Unable to create sstable representation %s: %s", name, e.getCause()), e.getCause());
        }
        catch (ReflectiveOperationException e)
        {
            throw new ConfigurationException(format("Unable to create sstable representation %s", name), e);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof RepresentationParams))
            return false;
        RepresentationParams that = (RepresentationParams) o;
        return required == that.required && Objects.equals(className, that.className) && options.equals(that.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(className, required, options);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add(CLASS, className)
                          .add(REQUIRED, required)
                          .add("options", options)
                          .toString();
    }
}
