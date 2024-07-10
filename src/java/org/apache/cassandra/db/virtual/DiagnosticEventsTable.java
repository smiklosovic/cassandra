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

package org.apache.cassandra.db.virtual;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.diag.DiagnosticEventPersistence;
import org.apache.cassandra.schema.TableMetadata;

public class DiagnosticEventsTable extends AbstractVirtualTable
{
    protected DiagnosticEventsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "diagnostic_events")
                           .comment("Diagnostic events")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn("class", UTF8Type.instance)
                           .addClusteringColumn("ts", TimestampType.instance)
                           .addClusteringColumn("type", UTF8Type.instance)
                           .addRegularColumn("value", UTF8Type.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        if (!DiagnosticEventPersistence.instance().isDiagnosticLogEnabled())
            return result;

        TreeMap<Long, Map<String, Serializable>> allEvents = DiagnosticEventPersistence.instance().getAllEvents();

        for (Map.Entry<Long, Map<String, Serializable>> entry : allEvents.entrySet())
        {
            Map<String, Serializable> event = entry.getValue();
            String clazz = (String) event.remove("class");
            String type = (String) event.remove("type");
            Long timestamp = (Long) event.remove("ts");
            event.remove("thread");

            result.row(clazz, new Date(timestamp), type).column("value", event.toString());
        }

        return result;
    }
}
