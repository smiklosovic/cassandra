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

package org.apache.cassandra.distributed.mock.nodetool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.InstanceAwareNodetool;
import org.apache.cassandra.distributed.shared.AbstractBuilder;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class NetstatsTest extends TestBaseImpl
{

    @Test
    public void testNetstatsStreamingProgress() throws Exception
    {
        final ExecutorService executorService = Executors.newFixedThreadPool(2);

        try (final InstanceAwareNodetool nodetool = new InstanceAwareNodetool(getCluster()))
        {
            final Cluster cluster = nodetool.getCluster();
            final IInvokableInstance node1 = cluster.get(1);
            final IInvokableInstance node2 = cluster.get(2);

            createTable(cluster);

            node1.flush("netstats_test");
            node2.flush("netstats_test");

            disableCompaction(node1);
            disableCompaction(node2);

            populateData();

            // change RF from 1 to 2 so we need to repair it, repairing will causes streaming shown in netstats
            changeReplicationFactor();

            final AtomicReference<Boolean> netstatsShouldRun = new AtomicReference<>();
            netstatsShouldRun.set(true);

            final Future<NetstatResults> resultsFuture1 = executorService.submit(new NetstatsCallable(netstatsShouldRun, nodetool, node1));
            final Future<NetstatResults> resultsFuture2 = executorService.submit(new NetstatsCallable(netstatsShouldRun, nodetool, node2));

            final IIsolatedExecutor.CallableNoExcept<?> repairCallable = node1.asyncCallsOnInstance(() -> {
                try
                {
                    StorageService.instance.repair("netstats_test", Collections.emptyMap(), Collections.emptyList()).right.get();

                    return null;
                }
                catch (final Exception ex)
                {
                    throw new RuntimeException(ex);
                }
            });

            repairCallable.call();

            Thread.currentThread().sleep(60000);

            netstatsShouldRun.set(false);

            NetstatResults results1 = resultsFuture1.get();
            NetstatResults results2 = resultsFuture2.get();

            results1.assertSuccessful();
            results2.assertSuccessful();
        }
        finally
        {
            executorService.shutdownNow();

            if (!executorService.isShutdown())
            {
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES))
                {
                    throw new IllegalStateException("Unable to shutdown executor for invoking netstat commands.");
                }
            }
        }
    }

    // helpers

    private static final class NetstatResults
    {
        private final List<String[]> netstatOutputs = new ArrayList<>();

        public void add(String[] result)
        {
            netstatOutputs.add(result);
        }

        public void assertSuccessful()
        {
            for (final String[] result : netstatOutputs)
            {
                InstanceAwareNodetool.assertSuccessful(result);
            }
        }
    }

    private static class NetstatsCallable implements Callable<NetstatResults>
    {
        private final AtomicReference<Boolean> netstatsShouldRun;
        private final IInvokableInstance node;
        private final InstanceAwareNodetool nodetool;

        public NetstatsCallable(final AtomicReference<Boolean> netstatsShouldRun,
                                final InstanceAwareNodetool nodetool,
                                final IInvokableInstance node)
        {
            this.netstatsShouldRun = netstatsShouldRun;
            this.nodetool = nodetool;
            this.node = node;
        }

        public NetstatResults call() throws Exception
        {
            final NetstatResults results = new NetstatResults();

            while (netstatsShouldRun.get())
            {
                results.add(nodetool.execute(node, "netstats"));
                Thread.currentThread().sleep(1000);
            }

            return results;
        }
    }

    private void changeReplicationFactor()
    {
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session s = c.connect())
        {
            s.execute("ALTER KEYSPACE netstats_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
        }
    }

    private void createTable(Cluster cluster)
    {
        // replication factor is 1
        cluster.schemaChange("CREATE KEYSPACE netstats_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        cluster.schemaChange("CREATE TABLE netstats_test.test_table (id uuid primary key);");
    }

    private void populateData()
    {
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session s = c.connect("netstats_test"))
        {

            for (int i = 0; i < 100000; i++)
            {
                s.execute("INSERT INTO test_table (id) VALUES (" + UUID.randomUUID() + ")");
            }
        }
    }

    private void disableCompaction(IInvokableInstance invokableInstance)
    {
        invokableInstance.runOnInstance(() -> {
            try
            {
                StorageService.instance.disableAutoCompaction("netstats_test");
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        });
    }

    private static AbstractBuilder<IInvokableInstance, Cluster, Cluster.Builder> getCluster()
    {
        return Cluster.build()
                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                      .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                  .set("stream_throughput_outbound_megabits_per_sec", 1)
                                                  .set("compaction_throughput_mb_per_sec", 1)
                                                  .set("stream_entire_sstables", false));
    }
}