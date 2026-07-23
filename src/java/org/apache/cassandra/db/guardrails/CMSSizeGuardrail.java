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

package org.apache.cassandra.db.guardrails;

import java.util.function.ToLongFunction;

import org.apache.cassandra.service.ClientState;

import static java.lang.String.format;

public class CMSSizeGuardrail extends MinThreshold
{
    /**
     * Creates a new minimum threshold guardrail.
     *
     * @param failThreshold   a {@link ClientState}-based provider of the value above which the operation should be aborted.
     */
    public CMSSizeGuardrail(ToLongFunction<ClientState> failThreshold)
    {
        super("minimum_cms_size",
              null,
              state -> -1,
              failThreshold,
              (isWarning, what, value, threshold) ->
              format("The CMS size of %s is below the failure threshold of %s. " +
                     "Reconfigure CMS so its total size (sum of replication factors across all datacenters) is at least %s.",
                     value, threshold, threshold));
        this.throwOnNullClientState = true;
    }

    public void guard(int minCmsSize, int totalNodes, int cmsSize)
    {
        // Skip clusters smaller than the threshold: they cannot host that many replicas yet (e.g. during bootstrap).
        // When the guardrail is disabled this is never true, and guard() below is then a no-op.
        if (totalNodes < minCmsSize)
            return;

        guard(cmsSize, "CMS", false, null);
    }
}
