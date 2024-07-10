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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditEvent;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.db.guardrails.GuardrailEvent;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.BootstrapEvent;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorEvent;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventPersistence;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.gms.GossiperEvent;
import org.apache.cassandra.hints.HintEvent;
import org.apache.cassandra.hints.HintsServiceEvent;
import org.apache.cassandra.schema.SchemaAnnouncementEvent;
import org.apache.cassandra.schema.SchemaEvent;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.repair.PartitionRepairEvent;
import org.apache.cassandra.service.reads.repair.ReadRepairEvent;

public class DiagnosticEventsSubscriptionsTable extends AbstractMutableVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticEventsSubscriptionsTable.class);

    private static final Map<Class<? extends DiagnosticEvent>, Set<Enum<?>>> ALL_EVENTS = new HashMap<>();

    {
        ALL_EVENTS.put(AuditEvent.class, Set.of(AuditLogEntryType.values()));
        ALL_EVENTS.put(BootstrapEvent.class, Set.of(BootstrapEvent.BootstrapEventType.values()));
        ALL_EVENTS.put(GossiperEvent.class, Set.of(GossiperEvent.GossiperEventType.values()));
        ALL_EVENTS.put(GuardrailEvent.class, Set.of(GuardrailEvent.GuardrailEventType.values()));
        ALL_EVENTS.put(HintEvent.class, Set.of(HintEvent.HintEventType.values()));
        ALL_EVENTS.put(HintsServiceEvent.class, Set.of(HintsServiceEvent.HintsServiceEventType.values()));
        ALL_EVENTS.put(PartitionRepairEvent.class, Set.of(PartitionRepairEvent.PartitionRepairEventType.values()));
        ALL_EVENTS.put(ReadRepairEvent.class, Set.of(ReadRepairEvent.ReadRepairEventType.values()));
        ALL_EVENTS.put(SchemaAnnouncementEvent.class, Set.of(SchemaAnnouncementEvent.SchemaAnnouncementEventType.values()));
        ALL_EVENTS.put(SchemaEvent.class, Set.of(SchemaEvent.SchemaEventType.values()));
        ALL_EVENTS.put(TokenAllocatorEvent.class, Set.of(TokenAllocatorEvent.TokenAllocatorEventType.values()));
    }

    protected DiagnosticEventsSubscriptionsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "diagnostic_events_subscriptions")
                           .comment("Diagnostic events subscriptions")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn("class", UTF8Type.instance)
                           .addClusteringColumn("type", UTF8Type.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataSet = new SimpleDataSet(metadata());

        DiagnosticEventService.instance().getAllEventClassesWithTypes().forEach((clazz, types) -> {
            for (Enum<?> type : types)
                dataSet.row(clazz.getSimpleName(), type.name());
        });

        return dataSet;
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        if (!DiagnosticEventService.instance().isDiagnosticsEnabled())
            return;

        if (!DiagnosticEventService.instance().isDiagnosticLogEnabled())
            return;

        String clazz = partitionKey.value(0);
        String type = clusteringColumns.value(0);

        Class<?> subscriptionClass = null;
        Set<Enum<?>> subscriptionTypes = new HashSet<>();

        for (Map.Entry<Class<? extends DiagnosticEvent>, Set<Enum<?>>> entry : ALL_EVENTS.entrySet())
        {
            if (subscriptionClass != null && !subscriptionTypes.isEmpty())
                break;

            Class<?> eventClass = entry.getKey();
            if (eventClass.getName().equalsIgnoreCase(clazz) || eventClass.getSimpleName().equalsIgnoreCase(clazz))
            {
                subscriptionClass = eventClass;

                if (type.equalsIgnoreCase("all"))
                {
                    subscriptionTypes.addAll(entry.getValue());
                }
                else
                {
                    for (Enum<?> typeEnum : entry.getValue())
                    {
                        if (typeEnum.name().equalsIgnoreCase(type))
                        {
                            subscriptionTypes.add(typeEnum);
                            break;
                        }
                    }
                }
            }
        }

        if (subscriptionClass == null || subscriptionTypes.isEmpty())
            return;

        for (Enum subscriptionType : subscriptionTypes)
            DiagnosticEventPersistence.instance().enableEventPersistence(subscriptionClass.getName(), subscriptionType);
    }
}
