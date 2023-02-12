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

package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface IDictionaryTrainer
{
    public static final IDictionaryTrainer NO_OP = new NoOpDictionaryTrainer();

    static class NoOpDictionaryTrainer implements IDictionaryTrainer
    {
        @Override
        public ByteBuffer getDictionary()
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        @Override
        public void trainDictionary()
        {

        }

        @Override
        public void addSample(byte[] sample)
        {

        }

        @Override
        public void addSample(Partition partition)
        {
        }

        @Override
        public boolean isTrained()
        {
            return true;
        }
    }

    boolean isTrained();

    ByteBuffer getDictionary();

    void trainDictionary();

    void addSample(byte[] sample);

    void addSample(Partition partition);
}
