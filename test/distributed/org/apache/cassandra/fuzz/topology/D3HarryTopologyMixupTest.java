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

package org.apache.cassandra.fuzz.topology;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;

import accord.utils.Property;

/**
 * SCRATCH TEST — created for the 2026-07 TCM deep review, Track D, agent D3.
 * Not part of the permanent suite; do not commit.
 *
 * Identical behavior to {@link HarryTopologyMixupTest} (RANDOM seed per JUnit
 * invocation, same 20 steps / 1 example / 3 min step timeout inherited from
 * TopologyMixupTestBase), except it:
 *
 *  1) Reflectively reads the (otherwise package-private-until-failure) seed
 *     chosen by accord.utils.Property.Common before the run starts, and
 *     records it to a known file so the seed of every run — pass or fail —
 *     can be captured, not just failing ones (Property only prints the seed
 *     inside PropertyError on failure; see accord/utils/Property.java:192).
 *  2) Optionally pins the seed via -Dd3.seed=<long> so a specific run can be
 *     reproduced deterministically for triage, without touching
 *     HarryTopologyMixupTest or the base class.
 *
 * Seed-record file path can be overridden with -Dd3.seedFile=<path>; default
 * is tcm-review/phase2/trackD/.current-seed relative to the working directory
 * ant is invoked from (the repo root).
 */
public class D3HarryTopologyMixupTest extends HarryTopologyMixupTest
{
    @Override
    protected void preCheck(Property.StatefulBuilder builder)
    {
        // Leave RANDOM by default (matches HarryTopologyMixupTest); allow a pin for repro.
        String pin = System.getProperty("d3.seed");
        if (pin != null)
            builder.withSeed(Long.parseLong(pin.trim()));

        long seed = readSeed(builder);
        recordSeed(seed);
    }

    private static long readSeed(Property.StatefulBuilder builder)
    {
        try
        {
            Field f = Property.Common.class.getDeclaredField("seed");
            f.setAccessible(true);
            return f.getLong(builder);
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException("D3HarryTopologyMixupTest: unable to reflect seed field off Property.Common", e);
        }
    }

    private static void recordSeed(long seed)
    {
        String path = System.getProperty("d3.seedFile", "tcm-review/phase2/trackD/.current-seed");
        try (FileWriter w = new FileWriter(path, false))
        {
            w.write(Long.toString(seed));
            w.write('\n');
        }
        catch (IOException e)
        {
            // Non-fatal: seed capture is best-effort instrumentation, must never fail the run.
            System.err.println("D3HarryTopologyMixupTest: failed to record seed " + seed + " to " + path + ": " + e);
        }
        // Also to stdout/log so it shows up in the captured test output regardless of file writability.
        System.out.println("D3HarryTopologyMixupTest: seed=" + seed);
    }
}
