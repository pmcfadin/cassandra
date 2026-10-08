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

package org.apache.cassandra.distributed.test;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

/**
 * Measures how the cost of a single CREATE TABLE scales with the number of tables that already
 * exist in the same keyspace.
 *
 * Each test method builds a fresh single node in-JVM cluster, creates one keyspace, then issues
 * N CREATE TABLE statements sequentially, timing each one individually. Results are printed as
 * a per-bucket mean (bucket == 100 consecutive statements) so the shape of the curve is visible:
 * a flat curve means CREATE TABLE is O(1) in the existing table count and the total is linear,
 * a rising curve means the total is super-linear.
 *
 * Statements are executed inside the instance (parse + validate + executeLocally, exactly what
 * {@code Instance#schemaChangeInternal} does) so that the numbers do not include cross-classloader
 * marshalling or the dtest schema-agreement monitor.
 *
 * Run with, e.g.:
 *   ant test-jvm-dtest-some -Dtest.name=org.apache.cassandra.distributed.test.ManyTablesScalingTest -Dtest.methods=n2000
 */
public class ManyTablesScalingTest extends TestBaseImpl
{
    private static final int BUCKET = 100;

    @Test
    public void n250() throws Throwable   { run(250); }

    @Test
    public void n500() throws Throwable   { run(500); }

    @Test
    public void n1000() throws Throwable  { run(1000); }

    @Test
    public void n2000() throws Throwable  { run(2000); }

    @Test
    public void n5000() throws Throwable  { run(5000); }

    private static void run(int n) throws IOException
    {
        try (Cluster cluster = Cluster.build(1).start())
        {
            cluster.schemaChange("CREATE KEYSPACE mt WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

            String outDir = System.getProperty("manytables.out");
            final String progress = outDir == null ? null : outDir + "/progress-" + n + ".csv";

            long wallStart = System.nanoTime();
            long[][] result = cluster.get(1).callOnInstance(() -> {
                com.sun.management.ThreadMXBean tmx =
                    (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
                tmx.setThreadAllocatedMemoryEnabled(true);

                long[] nanos = new long[n];
                long[] allocAt = new long[n + 1];

                org.apache.cassandra.service.ClientState state =
                    org.apache.cassandra.service.ClientState.forInternalCalls(org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME);
                org.apache.cassandra.service.QueryState queryState = new org.apache.cassandra.service.QueryState(state);
                org.apache.cassandra.cql3.QueryOptions options =
                    org.apache.cassandra.cql3.QueryOptions.forInternalCalls(java.util.Collections.emptyList());

                long[] ids = tmx.getAllThreadIds();
                long[] bytes = tmx.getThreadAllocatedBytes(ids);
                long sum = 0;
                for (long b : bytes) if (b > 0) sum += b;
                allocAt[0] = sum;

                for (int i = 0; i < n; i++)
                {
                    String cql = "CREATE TABLE mt.t" + i + " (k int PRIMARY KEY, v text)";
                    long t0 = System.nanoTime();
                    org.apache.cassandra.cql3.CQLStatement stmt =
                        org.apache.cassandra.cql3.QueryProcessor.parseStatement(cql, queryState.getClientState());
                    stmt.validate(state);
                    stmt.executeLocally(queryState, options);
                    nanos[i] = System.nanoTime() - t0;

                    ids = tmx.getAllThreadIds();
                    bytes = tmx.getThreadAllocatedBytes(ids);
                    sum = 0;
                    for (long b : bytes) if (b > 0) sum += b;
                    allocAt[i + 1] = sum;

                    if (progress != null && (i + 1) % 100 == 0)
                    {
                        int from = i + 1 - 100;
                        long bs = 0;
                        long[] slice = new long[100];
                        for (int j = 0; j < 100; j++) { slice[j] = nanos[from + j]; bs += slice[j]; }
                        java.util.Arrays.sort(slice);
                        String line = String.format("%d,%d,%.2f,%.2f,%.2f%n",
                                                    from + 1, i + 1,
                                                    bs / 100.0 / 1e6,
                                                    slice[50] / 1e6,
                                                    (allocAt[i + 1] - allocAt[from]) / 104857600.0);
                        try
                        {
                            java.nio.file.Files.write(java.nio.file.Paths.get(progress),
                                                      line.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                                      java.nio.file.StandardOpenOption.CREATE,
                                                      java.nio.file.StandardOpenOption.APPEND);
                        }
                        catch (java.io.IOException e) { throw new RuntimeException(e); }
                    }
                }
                return new long[][]{ nanos, allocAt };
            });
            long wallNanos = System.nanoTime() - wallStart;

            long[] nanos = result[0];
            long[] allocAt = result[1];

            long totalNanos = 0;
            for (long v : nanos) totalNanos += v;

            StringBuilder sb = new StringBuilder();
            sb.append("\n#### MANYTABLES N=").append(n).append('\n');
            sb.append(String.format("#### total wall (incl. instance call): %.3f s%n", wallNanos / 1e9));
            sb.append(String.format("#### total of timed statements:       %.3f s%n", totalNanos / 1e9));
            sb.append(String.format("#### total allocated (approx):        %.1f MiB%n",
                                    (allocAt[n] - allocAt[0]) / 1048576.0));
            sb.append("#### bucket,from,to,mean_ms,median_ms,min_ms,max_ms,alloc_MiB\n");
            for (int start = 0; start < n; start += BUCKET)
            {
                int end = Math.min(start + BUCKET, n);
                int len = end - start;
                long[] slice = new long[len];
                System.arraycopy(nanos, start, slice, 0, len);
                long s = 0;
                for (long v : slice) s += v;
                java.util.Arrays.sort(slice);
                sb.append(String.format("#### %d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f%n",
                                        start / BUCKET,
                                        start + 1,
                                        end,
                                        s / (double) len / 1e6,
                                        slice[len / 2] / 1e6,
                                        slice[0] / 1e6,
                                        slice[len - 1] / 1e6,
                                        (allocAt[end] - allocAt[start]) / 1048576.0));
            }
            System.out.print(sb);
            System.out.flush();
            String out = System.getProperty("manytables.out");
            if (out != null)
            {
                java.nio.file.Files.write(java.nio.file.Paths.get(out + "/manytables-" + n + ".txt"),
                                          sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
    }
}
