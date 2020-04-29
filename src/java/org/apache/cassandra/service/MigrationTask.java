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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.SystemKeyspace.BootstrapState;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.utils.WrappedRunnable;


public final class MigrationTask extends WrappedRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);

    private static final Set<BootstrapState> monitoringBootstrapStates = EnumSet.of(BootstrapState.NEEDS_BOOTSTRAP, BootstrapState.IN_PROGRESS);

    private static final Comparator<InetAddress> inetcomparator = new Comparator<InetAddress>()
    {
        public int compare(InetAddress addr1, InetAddress addr2)
        {
            return addr1.getHostAddress().compareTo(addr2.getHostAddress());
        }
    };

    private static final Set<InetAddress> infFlightRequests = new ConcurrentSkipListSet<InetAddress>(inetcomparator);

    private final InetAddress endpoint;
    private final IAsyncCallbackWithFailure<Collection<Mutation>> cb;

    MigrationTask(InetAddress endpoint, IAsyncCallbackWithFailure<Collection<Mutation>> callback)
    {
        this.endpoint = endpoint;
        this.cb = callback;
    }

    public static boolean addInFlightSchemaRequest(InetAddress ep)
    {
        return infFlightRequests.add(ep);
    }

    public static void completedInFlightSchemaRequest(InetAddress ep)
    {
        infFlightRequests.remove(ep);
    }

    public static boolean hasInFlighSchemaRequest(InetAddress ep)
    {
        return infFlightRequests.contains(ep);
    }

    public void runMayThrow()
    {
        if (!FailureDetector.instance.isAlive(endpoint))
        {
            logger.warn("Can't send schema pull request: node {} is down.", endpoint);
            return;
        }

        // There is a chance that quite some time could have passed between now and the MM#maybeScheduleSchemaPull(),
        // potentially enough for the endpoint node to restart - which is an issue if it does restart upgraded, with
        // a higher major.
        if (!MigrationManager.shouldPullSchemaFrom(endpoint))
        {
            logger.info("Skipped sending a migration request: node {} has a higher major version now.", endpoint);
            return;
        }

        if (monitoringBootstrapStates.contains(SystemKeyspace.getBootstrapState()) && !addInFlightSchemaRequest(endpoint))
        {
            logger.info("Skipped sending a migration request: node {} already has a request in flight", endpoint);
            return;
        }

        MessageOut<?> message = new MessageOut<>(MessagingService.Verb.MIGRATION_REQUEST, null, MigrationManager.MigrationsSerializer.instance);

        logger.info("Sending schema pull request to {} at {} with timeout {}", endpoint, System.currentTimeMillis(), message.getTimeout());

        MessagingService.instance().sendRR(message, endpoint, cb, message.getTimeout(), true);
    }

    /**
     * onFailure method on migration callback is called in two situations. The first one is the "happy path"
     * when a message come through and callback is not expired yet. Check {@link org.apache.cassandra.net.ResponseVerbHandler}.
     *
     * In case it is not invoked on happy path, the second case it is called is upon its expiration from ExpirationMap
     * in {@link MessagingService}, it its constructor, there is as function which is called regularly and checks
     * if some callbacks are eligible to be removed (expired) or not. If they are and a callback is a failureCallback
     * (which this callback is), its onFailure method is invoked.
     *
     * Hence, it does not matter if a callback is expired or not, its onFailure method will be eventually call one way
     * or the other. This means that respective endpoint to check a schema agreement of will be tried out again.
     *
     * In reponse method, we are merging remote schema with a local one. If this merge is not successful, schemas
     * will not match hence whole algorithm will be triggered again until they do or until timeout is reached.
     */
    public static class MigrationTaskCallback implements IAsyncCallbackWithFailure<Collection<Mutation>>
    {
        private static final Logger logger = LoggerFactory.getLogger(MigrationTaskCallback.class);

        private final InetAddress endpoint;

        public MigrationTaskCallback(InetAddress endpoint)
        {
            this.endpoint = endpoint;
        }

        public InetAddress getEndpoint()
        {
            return endpoint;
        }

        @Override
        public void response(MessageIn<Collection<Mutation>> message)
        {
            try
            {
                logger.trace("Received response to schema request from {} at {}", message.from, System.currentTimeMillis());
                SchemaKeyspace.mergeSchemaAndAnnounceVersion(message.payload);
            }
            catch (ConfigurationException e)
            {
                logger.error("Configuration exception merging remote schema", e);
            }
            finally
            {
                completedInFlightSchemaRequest(endpoint);
            }
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }

        public void onFailure(InetAddress from, RequestFailureReason failureReason)
        {
            logger.warn("Timed out waiting for schema response from {} at {}", endpoint, System.currentTimeMillis());
            completedInFlightSchemaRequest(endpoint);
        }
    }
}
