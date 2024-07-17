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

import org.apache.cassandra.audit.AuditEvent;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.guardrails.GuardrailEvent;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.BootstrapEvent;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorEvent;
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
    private static final Map<String, Set<Enum<?>>> ALL_EVENTS = new HashMap<>();

    {
        ALL_EVENTS.put(AuditEvent.class.getName(), Set.of(AuditLogEntryType.values()));
        ALL_EVENTS.put(BootstrapEvent.class.getName(), Set.of(BootstrapEvent.BootstrapEventType.values()));
        ALL_EVENTS.put(GossiperEvent.class.getName(), Set.of(GossiperEvent.GossiperEventType.values()));
        ALL_EVENTS.put(GuardrailEvent.class.getName(), Set.of(GuardrailEvent.GuardrailEventType.values()));
        ALL_EVENTS.put(HintEvent.class.getName(), Set.of(HintEvent.HintEventType.values()));
        ALL_EVENTS.put(HintsServiceEvent.class.getName(), Set.of(HintsServiceEvent.HintsServiceEventType.values()));
        ALL_EVENTS.put(PartitionRepairEvent.class.getName(), Set.of(PartitionRepairEvent.PartitionRepairEventType.values()));
        ALL_EVENTS.put(ReadRepairEvent.class.getName(), Set.of(ReadRepairEvent.ReadRepairEventType.values()));
        ALL_EVENTS.put(SchemaAnnouncementEvent.class.getName(), Set.of(SchemaAnnouncementEvent.SchemaAnnouncementEventType.values()));
        ALL_EVENTS.put(SchemaEvent.class.getName(), Set.of(SchemaEvent.SchemaEventType.values()));
        ALL_EVENTS.put(TokenAllocatorEvent.class.getName(), Set.of(TokenAllocatorEvent.TokenAllocatorEventType.values()));
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
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet dataSet = new SimpleDataSet(metadata());
        String eventClass = UTF8Type.instance.getString(partitionKey.getKey());

        for (Enum<?> entry : DiagnosticEventService.instance().getEventTypesByClass(eventClass))
            dataSet.row(eventClass, entry.name());

        return dataSet;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataSet = new SimpleDataSet(metadata());

        DiagnosticEventService.instance().getAllEventClassesWithTypes().forEach((clazz, types) -> {
            for (Enum<?> type : types)
                dataSet.row(clazz.getName(), type.name());
        });

        return dataSet;
    }

    @Override
    protected void applyPartitionDeletion(ColumnValues partitionKey)
    {
        String clazz = partitionKey.value(0);

        for (Enum type : DiagnosticEventService.instance().getEventTypesByClass(clazz))
            DiagnosticEventPersistence.instance().disableEventPersistence(clazz, type);
    }

    @Override
    protected void applyRowDeletion(ColumnValues partitionKey, ColumnValues clusteringColumns)
    {
        String clazz = partitionKey.value(0);
        String type = clusteringColumns.value(0);

        for (Map.Entry<Class<?>, Set<Enum<?>>> entry : DiagnosticEventService.instance().getAllEventClassesWithTypes().entrySet())
        {
            if (entry.getKey().getName().endsWith(clazz))
            {
                for (Enum enumType : entry.getValue())
                {
                    if (enumType.name().equals(type))
                    {
                        DiagnosticEventPersistence.instance().disableEventPersistence(entry.getKey().getName(), enumType);
                        return;
                    }
                }
            }
        }
    }

    @Override
    public void truncate()
    {
        for (Map.Entry<Class<?>, Set<Enum<?>>> entry : DiagnosticEventService.instance().getAllEventClassesWithTypes().entrySet())
        {
            for (Enum type : entry.getValue())
                DiagnosticEventPersistence.instance().disableEventPersistence(entry.getKey().getName(), type);
        }
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {

        String clazz = partitionKey.value(0);
        String type = clusteringColumns.value(0);

        Map<String, Set<Enum<?>>> subscriptions = new HashMap<>();

        for (Map.Entry<String, Set<Enum<?>>> entry : ALL_EVENTS.entrySet())
        {
            String eventClassName = entry.getKey();
            if (eventClassName.endsWith(clazz) || clazz.equalsIgnoreCase("all"))
            {
                subscriptions.put(eventClassName, new HashSet<>());

                if (type.equalsIgnoreCase("all"))
                {
                    subscriptions.get(eventClassName).addAll(entry.getValue());
                }
                else
                {
                    for (Enum<?> typeEnum : entry.getValue())
                    {
                        if (typeEnum.name().equals(type))
                            subscriptions.get(eventClassName).add(typeEnum);
                    }
                }
            }
        }

        for (Map.Entry<String, Set<Enum<?>>> entry : subscriptions.entrySet())
        {
            String eventClass = entry.getKey();
            Set<Enum<?>> eventTypes = entry.getValue();

            for (Enum eventType : eventTypes)
                DiagnosticEventPersistence.instance().enableEventPersistence(eventClass, eventType);
        }
    }
}
