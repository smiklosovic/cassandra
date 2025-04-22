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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.tracing.TraceKeyspace;

import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

/**
 * When adding new String keyspace names here, double check if it needs to be added to PartitionDenylist.canDenylistKeyspace
 */
public final class SchemaConstants
{
    public static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
    public static final Pattern PATTERN_NON_WORD_CHAR = Pattern.compile("\\W");


    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String SCHEMA_KEYSPACE_NAME = "system_schema";
    public static final String METADATA_KEYSPACE_NAME = "system_cluster_metadata";

    public static final String TRACE_KEYSPACE_NAME = "system_traces";
    public static final String ACCORD_KEYSPACE_NAME = "system_accord";
    public static final String AUTH_KEYSPACE_NAME = "system_auth";
    public static final String DISTRIBUTED_KEYSPACE_NAME = "system_distributed";

    public static final String VIRTUAL_SCHEMA = "system_virtual_schema";
    public static final String VIRTUAL_VIEWS = "system_views";
    public static final String VIRTUAL_METRICS = "system_metrics";
    public static final String VIRTUAL_ACCORD_DEBUG = "system_accord_debug";

    public static final String DUMMY_KEYSPACE_OR_TABLE_NAME = "--dummy--";

    /* system keyspace names (the ones with LocalStrategy replication strategy) */
    public static final Set<String> LOCAL_SYSTEM_KEYSPACE_NAMES =
        ImmutableSet.of(SYSTEM_KEYSPACE_NAME, SCHEMA_KEYSPACE_NAME, ACCORD_KEYSPACE_NAME);

    /* virtual table system keyspace names */
    public static final Set<String> VIRTUAL_SYSTEM_KEYSPACE_NAMES =
        ImmutableSet.of(VIRTUAL_SCHEMA, VIRTUAL_VIEWS, VIRTUAL_METRICS);

    /* replicate system keyspace names (the ones with a "true" replication strategy) */
    public static final Set<String> REPLICATED_SYSTEM_KEYSPACE_NAMES =
        ImmutableSet.of(TRACE_KEYSPACE_NAME, AUTH_KEYSPACE_NAME, DISTRIBUTED_KEYSPACE_NAME, METADATA_KEYSPACE_NAME);
    /**
     * The longest permissible KS or CF name.
     *
     * Before CASSANDRA-16956, we used to care about not having the entire path longer than 255 characters because of
     * Windows support but this limit is by implementing CASSANDRA-16956 not in effect anymore.
     */
    public static final int NAME_LENGTH = 48;

    /**
     * Longest acceptable file name. Longer names lead to too long file name error.
     */
    public static final int FILENAME_LENGTH = 255;

    /**
     * Longest acceptable table name, so it can be used in a directory
     * name constructed with a suffix of a table id and a separator.
     */
    public static final int TABLE_NAME_LENGTH = FILENAME_LENGTH - 32 - 1;

    // 59adb24e-f3cd-3e02-97f0-5b395827453f
    public static final UUID emptyVersion;

    public static final List<String> LEGACY_AUTH_TABLES = Arrays.asList("credentials", "users", "permissions");

    /**
     * Validates that a name is valid to be used in files. Assumes that the name length
     * should fit the default, {@link #NAME_LENGTH}.
     * See {@link #isValidName(String, int)} for more details.
     *
     * @param name the name to check
     * @return whether the name is safe for use in file paths and file names
     */
    public static boolean isValidName(String name)
    {
        return isValidName(name, NAME_LENGTH);
    }

    /**
     * Names such as keyspace, table, index names are used in file paths and file names,
     * so, they need to be safe for the use there, i.e., short enough and
     * containing only alphanumeric characters and underscores.
     * Allows to provide the length of names, since it varies for different database objects,
     * especially, they were not historically controlled for {@link #NAME_LENGTH}, see,
     * e.g., CASSANDRA-20389.
     * There is a case when the length cannot be controlled by a single value. In such case
     * the length validation is skipped if the given length is smaller than 1.
     *
     * @param name      the name to check
     * @param maxLength max acceptable length for the given name. For the cases when it cannot
     *                  be limited, 0 or negative number should be supplied.
     * @return true if the name is valid, false otherwise
     */
    public static boolean isValidName(String name, int maxLength)
    {
        return name != null && !name.isEmpty() && PATTERN_WORD_CHARS.matcher(name).matches()
               && (maxLength <= 0 || name.length() <= maxLength);
    }

    static
    {
        emptyVersion = UUID.nameUUIDFromBytes(Digest.forSchema().digest());
    }

    /**
     * @return whether or not the keyspace is a really system one (w/ LocalStrategy, unmodifiable, hardcoded)
     */
    public static boolean isLocalSystemKeyspace(String keyspaceName)
    {
        return LOCAL_SYSTEM_KEYSPACE_NAMES.contains(toLowerCaseLocalized(keyspaceName)) || isVirtualSystemKeyspace(keyspaceName);
    }

    /**
     * @return whether or not the keyspace is a replicated system ks (system_auth, system_traces, system_distributed)
     */
    public static boolean isReplicatedSystemKeyspace(String keyspaceName)
    {
        return REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(toLowerCaseLocalized(keyspaceName));
    }

    /**
     * Checks if the keyspace is a virtual system keyspace.
     * @return {@code true} if the keyspace is a virtual system keyspace, {@code false} otherwise.
     */
    public static boolean isVirtualSystemKeyspace(String keyspaceName)
    {
        return VIRTUAL_SYSTEM_KEYSPACE_NAMES.contains(toLowerCaseLocalized(keyspaceName));
    }

    /**
     * Checks if the keyspace is a system keyspace (local replicated or virtual).
     * @return {@code true} if the keyspace is a system keyspace, {@code false} otherwise.
     */
    public static boolean isSystemKeyspace(String keyspaceName)
    {
        return isLocalSystemKeyspace(keyspaceName) // this includes vtables
                || isReplicatedSystemKeyspace(keyspaceName);
    }

    /**
     * @return whether or not the keyspace is a non-virtual, system keyspace
     */
    public static boolean isNonVirtualSystemKeyspace(String keyspaceName)
    {
        final String lowercaseKeyspaceName = toLowerCaseLocalized(keyspaceName);
        return LOCAL_SYSTEM_KEYSPACE_NAMES.contains(lowercaseKeyspaceName)
               || REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(lowercaseKeyspaceName);
    }

    /**
     * Returns the set of all system keyspaces
     * @return all system keyspaces
     */
    public static Set<String> getSystemKeyspaces()
    {
        return Sets.union(Sets.union(LOCAL_SYSTEM_KEYSPACE_NAMES, REPLICATED_SYSTEM_KEYSPACE_NAMES), VIRTUAL_SYSTEM_KEYSPACE_NAMES);
    }

    /**
     * Returns the set of local and replicated system keyspace names
     * @return all local and replicated system keyspace names
     */
    public static Set<String> getLocalAndReplicatedSystemKeyspaceNames()
    {
        return Sets.union(LOCAL_SYSTEM_KEYSPACE_NAMES, REPLICATED_SYSTEM_KEYSPACE_NAMES);
    }
    
    /**
     * Returns the set of all local and replicated system table names
     * @return all local and replicated system table names
     */
    public static Set<String> getLocalAndReplicatedSystemTableNames()
    {
        return ImmutableSet.<String>builder()
                           .addAll(SystemKeyspace.TABLE_NAMES)
                           .addAll(SchemaKeyspaceTables.ALL)
                           .addAll(TraceKeyspace.TABLE_NAMES)
                           .addAll(AuthKeyspace.TABLE_NAMES)
                           .addAll(SystemDistributedKeyspace.TABLE_NAMES)
                           .addAll(AccordKeyspace.TABLE_NAMES)
                           .build();
    }
}
