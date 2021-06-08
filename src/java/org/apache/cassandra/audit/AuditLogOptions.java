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
package org.apache.cassandra.audit;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.utils.binlog.BinLogOptions;

public class AuditLogOptions extends BinLogOptions
{
    public volatile boolean enabled = false;
    public ParameterizedClass logger = new ParameterizedClass(BinAuditLogger.class.getSimpleName(), Collections.emptyMap());
    public String included_keyspaces = StringUtils.EMPTY;
    // CASSANDRA-14498: By default, system, system_schema and system_virtual_schema are excluded, but these can be included via cassandra.yaml
    public String excluded_keyspaces = "system,system_schema,system_virtual_schema";
    public String included_categories = StringUtils.EMPTY;
    public String excluded_categories = StringUtils.EMPTY;
    public String included_users = StringUtils.EMPTY;
    public String excluded_users = StringUtils.EMPTY;

    /**
     * AuditLogs directory can be configured using `cassandra.logdir.audit` or default is set to `cassandra.logdir` + /audit/
     */
    public String audit_logs_dir = System.getProperty("cassandra.logdir.audit",
                                                      System.getProperty("cassandra.logdir",".")+"/audit/");

    public static class Builder
    {
        private boolean enabled;
        private ParameterizedClass logger;
        private String includedKeyspaces;
        private String excludedKeyspaces;
        private String includedCategories;
        private String excludedCategories;
        private String includedUsers;
        private String excludedUsers;

        public Builder()
        {
            this(new AuditLogOptions());
        }

        public Builder(final AuditLogOptions defaultOptions)
        {
            this.enabled = defaultOptions.enabled;
            this.logger = defaultOptions.logger;
            this.includedKeyspaces = defaultOptions.included_keyspaces;
            this.excludedKeyspaces = defaultOptions.excluded_keyspaces;
            this.includedCategories = defaultOptions.included_categories;
            this.excludedCategories = defaultOptions.excluded_categories;
            this.includedUsers = defaultOptions.included_users;
            this.excludedUsers = defaultOptions.excluded_users;
        }

        public Builder withEnabled(boolean enabled)
        {
            this.enabled = enabled;
            return this;
        }

        public Builder withLogger(final String loggerName, Map<String, String> parameters)
        {
            if (loggerName != null && !loggerName.trim().isEmpty())
                this.logger = new ParameterizedClass(loggerName.trim(), parameters);

            return this;
        }

        public Builder withIncludedKeyspaces(final String includedKeyspaces)
        {
            this.includedKeyspaces = sanitise(includedKeyspaces);
            return this;
        }

        public Builder withExcludedKeyspaces(final String excludedKeyspaces)
        {
            this.excludedKeyspaces = sanitise(excludedKeyspaces);
            return this;
        }

        public Builder withIncludedCategories(final String includedCategories)
        {
            this.includedCategories = sanitise(includedCategories);
            return this;
        }

        public Builder withExcludedCategories(final String excludedCategories)
        {
            this.excludedCategories = sanitise(excludedCategories);
            return this;
        }

        public Builder withIncludedUsers(final String includedUsers)
        {
            this.includedUsers = sanitise(includedUsers);
            return this;
        }

        public Builder withExcludedUsers(final String excludedUsers)
        {
            this.excludedUsers = sanitise(excludedUsers);
            return this;
        }

        public AuditLogOptions build()
        {
            final AuditLogOptions auditLogOptions = new AuditLogOptions();

            auditLogOptions.enabled = this.enabled;
            auditLogOptions.logger = this.logger;
            auditLogOptions.included_keyspaces = this.includedKeyspaces;
            auditLogOptions.excluded_keyspaces = this.excludedKeyspaces;
            auditLogOptions.included_categories = this.includedCategories;
            auditLogOptions.excluded_categories = this.excludedCategories;
            auditLogOptions.included_users = this.includedUsers;
            auditLogOptions.excluded_users = this.excludedUsers;

            return auditLogOptions;
        }

        public static AuditLogOptions sanitise(final AuditLogOptions other)
        {
            final AuditLogOptions options = new AuditLogOptions();

            options.enabled = other.enabled;
            options.logger = other.logger;
            options.included_keyspaces = sanitise(other.included_keyspaces);
            options.excluded_keyspaces = sanitise(other.excluded_keyspaces);
            options.included_categories = sanitise(other.included_categories);
            options.excluded_categories = sanitise(other.excluded_categories);
            options.included_users = sanitise(other.included_users);
            options.excluded_users = sanitise(other.excluded_users);

            return options;
        }

        private static String sanitise(final String input)
        {
            if (input == null || input.trim().isEmpty())
                return StringUtils.EMPTY;

            return Arrays.stream(input.split(","))
                         .map(String::trim)
                         .map(Strings::emptyToNull)
                         .filter(Objects::nonNull)
                         .collect(Collectors.joining(","));
        }
    }

    public String toString()
    {
        return "AuditLogOptions{" +
               "enabled=" + enabled +
               ", logger='" + logger + '\'' +
               ", included_keyspaces='" + included_keyspaces + '\'' +
               ", excluded_keyspaces='" + excluded_keyspaces + '\'' +
               ", included_categories='" + included_categories + '\'' +
               ", excluded_categories='" + excluded_categories + '\'' +
               ", included_users='" + included_users + '\'' +
               ", excluded_users='" + excluded_users + '\'' +
               ", audit_logs_dir='" + audit_logs_dir + '\'' +
               ", archive_command='" + archive_command + '\'' +
               ", roll_cycle='" + roll_cycle + '\'' +
               ", block=" + block +
               ", max_queue_weight=" + max_queue_weight +
               ", max_log_size=" + max_log_size +
               '}';
    }
}
