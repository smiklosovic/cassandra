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

import java.util.function.Consumer;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.lang.String.format;
import static org.apache.cassandra.db.Directories.SECONDARY_INDEX_NAME_SEPARATOR;
import static org.apache.cassandra.schema.SchemaConstants.FILENAME_LENGTH;
import static org.apache.cassandra.schema.SchemaConstants.TABLE_NAME_LENGTH;

public class SchemaUtils
{
    public static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
    public static final Pattern PATTERN_NON_WORD_CHAR = Pattern.compile("\\W");
    public static final String INVALID_CHARS_CUSTOM_INDEX_TARGET = "Column '%s' contains non-alphanumeric-underscore characters";
    public static final String TOO_LONG_CUSTOM_INDEX_TARGET = "Column '%s' is longer than the permissible name length of %d characters";
    public static final String KEYSPACE_NAME_INALID_TEMPLATE = "Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")";
    public static final String TABLE_NAME_INVALID_TEMPLATE = "Table name must not be empty or not contain non-alphanumeric-underscore characters (got \"%s\")";
    public static final String TABLE_NAME_TOO_LONG_TEMPLATE = "Table name must not be more than %d characters long (got %d characters for \"%s\")";
    public static final String INVALID_GENERATED_DIRECTORY_NAME_TEMPLATE = "Generated directory name for a table of %d characters doesn't fit the max filename legnth of %s. This unexpectedly wasn't prevented by check of the table name length, %d, to fit %d characters (got table name \"%s\" and generated directory name \"%s\"";

    /**
     * Validates that a name contains only alphanummeric characters or underscore,
     * so it can be used in file or directory names.
     *
     * @param name the name to check
     * @return whether the name contains only valid characters
     */
    public static boolean isValidName(String name)
    {
        return name != null && !name.isEmpty() && PATTERN_WORD_CHARS.matcher(name).matches();
    }

    public static void validateKeyspaceName(String keyspaceName)
    {
        validateKeyspaceName(keyspaceName, (message) -> { throw new ConfigurationException(message); }, KEYSPACE_NAME_INALID_TEMPLATE);
    }

    public static void validateKeyspaceName(String keyspaceName, Consumer<String> c, String messageTemplate)
    {
        if (!(isValidName(keyspaceName) && keyspaceName.length() <= SchemaConstants.NAME_LENGTH))
            c.accept(String.format(messageTemplate, SchemaConstants.NAME_LENGTH, keyspaceName));
    }

    public static void validateTableName(TableMetadata metadata)
    {
        String keyspace = metadata.keyspace;
        String name = metadata.name;

        validateKeyspaceName(metadata.keyspace, (message) -> { throw new ConfigurationException(message); }, keyspace + '.' + name + ": " + KEYSPACE_NAME_INALID_TEMPLATE);

        if (!isValidName(name))
            throw new ConfigurationException(keyspace + '.' + name + ": " + format(TABLE_NAME_INVALID_TEMPLATE, name));

        if (name.length() > TABLE_NAME_LENGTH)
            throw new ConfigurationException(keyspace + '.' + name + ": " + format(TABLE_NAME_TOO_LONG_TEMPLATE, TABLE_NAME_LENGTH, name.length(), name));

        String tableDirectoryName = getTableDirectoryName(name, metadata.id);
        int length = tableDirectoryName.length();

        assert length <= FILENAME_LENGTH : String.format(INVALID_GENERATED_DIRECTORY_NAME_TEMPLATE,
                                                         length,
                                                         FILENAME_LENGTH,
                                                         name.length(),
                                                         TABLE_NAME_LENGTH,
                                                         name,
                                                         tableDirectoryName);
    }

    public static void validateCustomIndexColumnName(String name)
    {
        if (!isValidName(name))
            throw new InvalidRequestException(String.format(INVALID_CHARS_CUSTOM_INDEX_TARGET, name));
        if (name.length() > SchemaConstants.NAME_LENGTH)
            throw new InvalidRequestException(String.format(TOO_LONG_CUSTOM_INDEX_TARGET, name, SchemaConstants.NAME_LENGTH));
    }

    /**
     * Returns the table part of the index table name or the entire table name
     * if not an index table.
     *
     * @return table name part
     */
    public static String getTableName(String name)
    {
        int idx = name.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
        return idx >= 0 ? name.substring(0, idx) : name;
    }

    /**
     * Generates directory name for the table by using table part of
     * the (index) table name and table id.
     *
     * @return directory name
     */
    public static String getTableDirectoryName(String name, TableId id)
    {
        return getTableName(name) + '-' + id.toHexString();
    }

    /**
     * Returns the index name from the name of an index table
     * including the dot prexing the index name.
     * If not an index table, returns null.
     *
     * @return index name prefixed with dot prefix or null
     */
    @Nullable
    public static String getIndexNameWithDot(String name)
    {
        int idx = name.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
        return idx >= 0 ? name.substring(idx) : null;
    }
}
