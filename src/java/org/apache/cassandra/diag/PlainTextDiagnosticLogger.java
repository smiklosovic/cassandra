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

package org.apache.cassandra.diag;

import java.util.Map;
import java.util.Optional;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;

public class PlainTextDiagnosticLogger extends RollingFileAppender<ILoggingEvent> implements IDiagnosticLogger
{
    public static final String APPENDER_NAME = "DIAGLOG";

    private DiagnosticLogOptions diagnosticLogOptions;
    private Marker diagnosticMarker;

    public PlainTextDiagnosticLogger()
    {
        diagnosticLogOptions = new DiagnosticLogOptions();
        diagnosticLogOptions.enabled = true;
        diagnosticLogOptions.logger = new ParameterizedClass(PlainTextDiagnosticLogger.class.getName(),
                                                             diagnosticLogOptions.toMap());

        diagnosticMarker = MarkerFactory.getMarker("DIAG");
    }

    public PlainTextDiagnosticLogger(Map<String, String> options)
    {
        diagnosticLogOptions = DiagnosticLogOptions.fromMap(options);
        diagnosticMarker = MarkerFactory.getMarker("DIAG");
    }

    @Override
    public void accept(DiagnosticEvent diagnosticEvent)
    {
        if (!isEnabled() || diagnosticEvent == null || !DiagnosticEventService.instance().isEnabled(diagnosticEvent.getClass()))
            return;

        LoggingEvent loggingEvent = new LoggingEvent();
        loggingEvent.setMessage(diagnosticEvent.getLogString());
        loggingEvent.setTimeStamp(diagnosticEvent.timestamp);
        loggingEvent.setLevel(Level.INFO);
        loggingEvent.setLoggerName(PlainTextDiagnosticLogger.class.getName());
        loggingEvent.setThreadName(Thread.currentThread().getName());
        loggingEvent.setCallerData(new StackTraceElement[]{});
        loggingEvent.setMarker(diagnosticMarker);
        this.append(loggingEvent);
    }

    @Override
    protected void append(ILoggingEvent eventObject)
    {
        if (eventObject.getMarker() != null && eventObject.getMarker().contains(diagnosticMarker))
            super.append(eventObject);
    }

    @Override
    public boolean isEnabled()
    {
        return DatabaseDescriptor.diagnosticEventsEnabled() && diagnosticLogOptions.enabled;
    }

    @Override
    public void stop()
    {
        DiagnosticEventService.instance().unsubscribe(this);
        super.stop();
    }

    @Override
    public void start()
    {
        super.start();
    }

    @Override
    public boolean isStarted()
    {
        return super.isStarted();
    }

    @Override
    public Optional<DiagnosticLogOptions> getDiagnosticLogOptions()
    {
        return Optional.of(diagnosticLogOptions);
    }

    private static class DiagnosticLoggingEvent extends LoggingEvent
    {
    }
}
