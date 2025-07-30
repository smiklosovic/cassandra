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

package org.apache.cassandra.index;

import java.util.Collections;
import java.util.List;
import javax.management.JMX;
import javax.management.ObjectName;

import org.junit.Test;

import com.stratio.cassandra.lucene.IndexServiceMBean;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.junit.Assert.assertEquals;

public class LuceneIndexTest extends CQLTester
{
    @Test
    public void creatingIndexMarksTheIndexAsBuilt() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (id INT PRIMARY KEY, user TEXT, body TEXT, time TIMESTAMP) WITH gc_grace_seconds = 0;");
        String indexName = createIndex("CREATE CUSTOM INDEX tweets_index ON %s()\n" +
                                       "USING 'com.stratio.cassandra.lucene.Index'\n" +
                                       "WITH OPTIONS = {\n" +
                                       "   'refresh_seconds': '1',\n" +
                                       "   'schema': '{\n" +
                                       "      fields: {\n" +
                                       "         id: {type: \"integer\"},\n" +
                                       "         user: {type: \"string\"},\n" +
                                       "         body: {type: \"text\", analyzer: \"english\"},\n" +
                                       "         time: {type: \"date\", pattern: \"yyyy/MM/dd\"}\n" +
                                       "      }\n" +
                                       "   }'\n" +
                                       "};");

        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);

        execute("INSERT INTO %s (id , body , time, user) VALUES (1, 'hello world', toTimestamp(now()), 'stefanm') USING TTL 10;");
        execute("INSERT INTO %s (id , body , time, user) VALUES (2, 'hello world', toTimestamp(now()), 'stefanm') USING TTL 10;");

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, tableName);
        cfs.forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        String[] allDataFileLocations = DatabaseDescriptor.getAllDataFileLocations();

        Thread.sleep(20000);

        UntypedResultSet result = execute("SELECT * FROM %s WHERE expr(tweets_index, '{\n" +
                                          "   filter: {type: \"range\", field: \"time\", lower: \"2014/04/25\", upper: \"2025/09/01\"}\n" +
                                          "}');");

        IndexServiceMBean mBean = getMBean(tableName);

        long docsBeforeCompaction = mBean.getNumDocs();
        //assertEquals(1L, docsBeforeCompaction);

//        execute("DELETE FROM %s WHERE id = 1");
//        execute("DELETE FROM %s WHERE id = 2");

        cfs.forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // this sleep will make refreshing of an index done in timely manner
        // so next compaction will compact it in such a way that
        // number of documents will be zero
        Thread.sleep(10000);

        CompactionManager.instance.performMaximal(cfs, false);

        Thread.sleep(10000);

        mBean.commit();
        mBean.refresh();

        long docsAfterCompaction = mBean.getNumDocs();

        mBean.forceMerge(1, true);

        mBean.refresh();

        docsAfterCompaction = mBean.getNumDocs();


        mBean.forceMergeDeletes(true);

        docsAfterCompaction = mBean.getNumDocs();

        //assertTrue(result.isEmpty());
    }

    private static void assertMarkedAsBuilt(String indexName)
    {
        List<String> indexes = SystemKeyspace.getBuiltIndexes(KEYSPACE, Collections.singleton(indexName));
        assertEquals(1, indexes.size());
        assertEquals(indexName, indexes.get(0));
    }

    private IndexServiceMBean getMBean(String tableName) throws Throwable
    {
        return JMX.newMBeanProxy(MBeanWrapper.instance.getMBeanServer(),
                                 new ObjectName(String.format("com.stratio.cassandra.lucene:type=Lucene,keyspace=%s,table=%s,index=%s", KEYSPACE, tableName, "tweets_index")),
                                 IndexServiceMBean.class);
    }
}
