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

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.db.guardrails.GuardrailsCache;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;

public class GuardrailThresholdsTable extends AbstractMutableVirtualTable
{
    public static final String TABLE_NAME = "guardrails_thresholds";

    public static final String NAME_COLUMN = "name";
    public static final String VALUE_COLUMN = "value";

    private final GuardrailsCache cache = GuardrailsCache.instance;

    public GuardrailThresholdsTable()
    {
        this(VIRTUAL_VIEWS);
    }

    private static final TupleType tupleType = TupleType.getInstance(new TypeParser(format("(%s, %s)", UTF8Type.instance, UTF8Type.instance)));

    public GuardrailThresholdsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Guardrails configuration table for thresholds")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(VALUE_COLUMN, tupleType)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Map.Entry<String, List<Method>> entry : cache.getThresholdsGetters().entrySet())
        {
            String guardrailName = entry.getKey();
            List<Object> values = new ArrayList<>();
            for (Method method : entry.getValue())
                values.add(cache.invoke(method));

            result.row(guardrailName).column(VALUE_COLUMN,
                                             tupleType.pack(values.get(0) == null ? EMPTY_BYTE_BUFFER : UTF8Type.instance.decompose(values.get(0).toString()),
                                                            values.get(1) == null ? EMPTY_BYTE_BUFFER : UTF8Type.instance.decompose(values.get(1).toString())));
        }

        return result;
    }

    @Override
    protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
    {
        if (columnValue.isEmpty())
            return;

        String key = partitionKey.value(0);

        Method setter = cache.getSetter(key);

        if (setter == null)
            throw new InvalidRequestException(format("there is no associated setter for guardrail with name %s", guardrailName));

        ColumnValue value = columnValue.get();
        Object val = value.value();

        List<ByteBuffer> unpack = tupleType.unpack((ByteBuffer) val);

        ByteBuffer failBuffer = unpack.get(0);
        ByteBuffer warnBuffer = unpack.get(1);

        String failString = UTF8Type.instance.getString(failBuffer);
        String warnString = UTF8Type.instance.getString(warnBuffer);

        cache.invoke(setter, failString, warnString);

        try
        {
            cache.invoke(setter, failString, warnString);
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException(ex.getMessage());
        }
    }
}
