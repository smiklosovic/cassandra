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

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.log.AbstractBinLogger;
import org.apache.cassandra.utils.binlog.BinLog;

public class BinAuditLogger extends AbstractBinLogger<AuditLogEntry> implements IAuditLogger
{
    protected static final Logger logger = LoggerFactory.getLogger(BinAuditLogger.class);

    public static final long CURRENT_VERSION = 0;
    public static final String AUDITLOG_TYPE = "audit";
    public static final String AUDITLOG_MESSAGE = "message";

    public BinAuditLogger(Map<String, String> options)
    {
        super(options);
        AuditLogOptions auditLogOptions = AuditLogOptions.fromMap(options);
        this.binLog = new BinLog.Builder(auditLogOptions).path(File.getPath(auditLogOptions.audit_logs_dir)).build(false);
    }

    /**
     * Stop the audit log leaving behind any generated files.
     */
    public synchronized void stop()
    {
        try
        {
            logger.info("Deactivation of audit log requested.");
            if (binLog != null)
            {
                logger.info("Stopping audit logger");
                binLog.stop();
                binLog = null;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public boolean isEnabled()
    {
        return binLog != null;
    }

    @Override
    public void log(AuditLogEntry logEntry)
    {
        BinLog binLog = this.binLog;
        if (binLog == null || logEntry == null)
        {
            return;
        }
        binLog.logRecord(new Message(logEntry.getLogString(getKeyValueSeparator(), getFieldSeparator())));
    }

    @VisibleForTesting
    public static class Message extends AbstractMessage
    {
        public Message(String message)
        {
            super(message);
        }

        @Override
        protected long version()
        {
            return CURRENT_VERSION;
        }

        @Override
        protected String type()
        {
            return AUDITLOG_TYPE;
        }

        @Override
        public void writeMarshallablePayload(WireOut wire)
        {
            wire.write(AUDITLOG_MESSAGE).text(message);
        }
    }
}
