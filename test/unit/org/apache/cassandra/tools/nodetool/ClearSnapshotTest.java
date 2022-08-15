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

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import javax.management.openmbean.TabularData;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.snapshot.SnapshotManifest;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.Clock;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ClearSnapshotTest extends CQLTester
{
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    @Test
    public void testClearSnapshot_NoArgs()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clearsnapshot");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getCleanedStderr()).contains("Specify snapshot name or --all");
        
        tool = ToolRunner.invokeNodetool("clearsnapshot", "--all");
        tool.assertOnCleanExit();
    }

    @Test
    public void testClearSnapshot_AllAndName()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clearsnapshot", "-t", "some-name", "--all");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getCleanedStderr()).contains("Specify only one of snapshot name or --all");
    }

    @Test
    public void testClearSnapshot_RemoveByName()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot","-t","some-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();
        
        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails();
        assertThat(snapshots_before).containsKey("some-name");

        tool = ToolRunner.invokeNodetool("clearsnapshot","-t","some-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();
        
        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails();
        assertThat(snapshots_after).doesNotContainKey("some-name");
    }

    @Test
    public void testClearSnapshot_RemoveMultiple()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot","-t","some-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        tool = ToolRunner.invokeNodetool("snapshot","-t","some-other-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails();
        assertThat(snapshots_before).hasSize(2);

        tool = ToolRunner.invokeNodetool("clearsnapshot","--all");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();
        
        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails();
        assertThat(snapshots_after).isEmpty();
    }

    @Test
    public void testClearSnapshotWithOlderThanFlag() throws Throwable
    {
        String tableName = createTable(KEYSPACE, "CREATE TABLE %s (id int primary key)");
        execute("INSERT INTO %s (id) VALUES (?)", 1);
        flush(KEYSPACE);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot", "-t", "snapshot-to-clear", "-cf", tableName, "--", KEYSPACE);
        tool.assertOnCleanExit();
        tool = ToolRunner.invokeNodetool("snapshot", "-t", "some-other-snapshot", "-cf", tableName, "--", KEYSPACE);
        tool.assertOnCleanExit();
        tool = ToolRunner.invokeNodetool("snapshot", "-t", "last-snapshot", "-cf", tableName, "--", KEYSPACE);
        tool.assertOnCleanExit();

        Instant start = Instant.ofEpochMilli(Clock.Global.currentTimeMillis());

        String tableId = Keyspace.open(KEYSPACE).getMetadata().tables.get(tableName).get().id.asUUID().toString().replaceAll("-", "");

        rewriteManifest(tableId, DatabaseDescriptor.getAllDataFileLocations(),
                        tableName, "snapshot-to-clear",
                        start.minus(1, HOURS));

        rewriteManifest(tableId, DatabaseDescriptor.getAllDataFileLocations(),
                        tableName, "some-other-snapshot",
                        start.minus(30, MINUTES));

        // wait 10 seconds for the sake of the test
        await().until(() -> Instant.now().isAfter(start.plusSeconds(10)));

        // clear all snapshots older than 1 hour
        ToolRunner.invokeNodetool("clearsnapshot", "--older-than", "1h", "--all");

        await().until(() -> !ToolRunner.invokeNodetool("listsnapshots").getStdout().contains("snapshot-to-clear") &&
                            ToolRunner.invokeNodetool("listsnapshots").getStdout().contains("some-other-snapshot") &&
                            ToolRunner.invokeNodetool("listsnapshots").getStdout().contains("last-snapshot"));

        // clear all snapshots older than 30 minutes
        ToolRunner.invokeNodetool("clearsnapshot", "--older-than", "30m", "--all");

        await().until(() -> !ToolRunner.invokeNodetool("listsnapshots").getStdout().contains("snapshot-to-clear") &&
                            !ToolRunner.invokeNodetool("listsnapshots").getStdout().contains("some-other-snapshot") &&
                            ToolRunner.invokeNodetool("listsnapshots").getStdout().contains("last-snapshot"));

        await().until(() -> Instant.now().isAfter(start.plusSeconds(20)));

        // clear all snapshots older than current timestamp
        ToolRunner.invokeNodetool("clearsnapshot", "--older-than-timestamp",
                                  Long.toString(Instant.ofEpochSecond(Clock.Global.currentTimeMillis()).toEpochMilli() / 1000L),
                                  "--all");

        await().until(() -> !ToolRunner.invokeNodetool("listsnapshots").getStdout().contains("snapshot-to-clear") &&
                            !ToolRunner.invokeNodetool("listsnapshots").getStdout().contains("some-other-snapshot") &&
                            !ToolRunner.invokeNodetool("listsnapshots").getStdout().contains("last-snapshot"));
    }

    private void rewriteManifest(String tableId,
                                 String[] dataDirs,
                                 String tableName,
                                 String snapshotName,
                                 Instant createdAt) throws Exception
    {
        Path manifestPath = findManifest(dataDirs, tableId, tableName, snapshotName);
        SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(new File(manifestPath));
        SnapshotManifest manifestWithEphemeralFlag = new SnapshotManifest(manifest.files, null, createdAt, false);
        manifestWithEphemeralFlag.serializeToJsonFile(new File(manifestPath));
    }

    private Path findManifest(String[] dataDirs, String tableId, String tableName, String snapshotName)
    {
        for (String dataDir : dataDirs)
        {
            Path manifest = Paths.get(dataDir)
                                 .resolve(KEYSPACE)
                                 .resolve(format("%s-%s", tableName, tableId))
                                 .resolve("snapshots")
                                 .resolve(snapshotName)
                                 .resolve("manifest.json");

            if (Files.exists(manifest))
            {
                return manifest;
            }
        }

        throw new IllegalStateException("Unable to find manifest!");
    }
    
}
