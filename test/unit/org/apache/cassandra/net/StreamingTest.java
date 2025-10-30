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

package org.apache.cassandra.net;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.EventLoop;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StreamingTest
{
    private static final SocketFactory factory = new SocketFactory();

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @AfterClass
    public static void cleanup() throws InterruptedException
    {
        factory.shutdownNow();
    }

    @Test
    public void testIncompatibleVersion()
    {
        AcceptVersions acceptOutbound = new AcceptVersions(current_version + 1, current_version + 1);
        AcceptVersions acceptInbound = new AcceptVersions(minimum_version + 2, current_version + 3);
        assertThatThrownBy(() -> streamingConnect(acceptOutbound, acceptInbound))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("failed to connect to /127.0.0.1:7012 for streaming data, outcome: incompatible, closest supported version: 14, max messaging version: 14");
    }

    @Test
    public void testCompatibleVersion() throws Throwable
    {
        AcceptVersions acceptOutbound = new AcceptVersions(MessagingService.minimum_version, current_version + 1);
        AcceptVersions acceptInbound = new AcceptVersions(minimum_version + 2, current_version + 3);
        streamingConnect(acceptOutbound, acceptInbound);
    }

    private NettyStreamingChannel streamingConnect(AcceptVersions acceptOutbound, AcceptVersions acceptInbound) throws Throwable
    {
        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(acceptInbound));
        try
        {
            inbound.open();
            InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
            EventLoop eventLoop = factory.defaultGroup().next();

            return NettyStreamingConnectionFactory.connect(eventLoop,
                                                           new OutboundConnectionSettings(endpoint)
                                                           .withAcceptVersions(acceptOutbound)
                                                           .withDefaults(ConnectionCategory.STREAMING), 5,
                                                           StreamingChannel.Kind.FILE);
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }
}