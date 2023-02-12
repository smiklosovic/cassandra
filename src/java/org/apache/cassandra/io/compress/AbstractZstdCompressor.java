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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import com.github.luben.zstd.Zstd;

public abstract class AbstractZstdCompressor implements ICompressor
{
    // These might change with the version of Zstd we're using
    public static final int FAST_COMPRESSION_LEVEL = Zstd.minCompressionLevel();
    public static final int BEST_COMPRESSION_LEVEL = Zstd.maxCompressionLevel();

    // Compressor Defaults
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;
    public static final boolean ENABLE_CHECKSUM_FLAG = true;

    @VisibleForTesting
    public static final String COMPRESSION_LEVEL_OPTION_NAME = "compression_level";

    protected final int compressionLevel;
    protected final Set<Uses> recommendedUses;

    protected AbstractZstdCompressor(int compressionLevel, Set<Uses> recommendedUses)
    {
        this.compressionLevel = compressionLevel;
        this.recommendedUses = recommendedUses;
    }

    /**
     * Check if the given compression level is valid. This can be a negative value as well.
     *
     * @param level level to check validity of
     * @return true of level is valid, false otherwise
     */
    protected static boolean isValid(int level)
    {
        return (level >= FAST_COMPRESSION_LEVEL && level <= BEST_COMPRESSION_LEVEL);
    }

    /**
     * Parse the compression options
     *
     * @param options options to resolve compression level from
     * @return compression level
     */
    protected static int getOrDefaultCompressionLevel(Map<String, String> options)
    {
        if (options == null)
            return DEFAULT_COMPRESSION_LEVEL;

        String val = options.get(COMPRESSION_LEVEL_OPTION_NAME);

        if (val == null)
            return DEFAULT_COMPRESSION_LEVEL;

        return Integer.parseInt(val);
    }

    /**
     * Get initial compressed buffer length
     *
     * @param chunkLength lenght of chunk
     * @return initial compressed buffer length
     */
    @Override
    public int initialCompressedBufferLength(int chunkLength)
    {
        return (int) Zstd.compressBound(chunkLength);
    }


    /**
     * Return the preferred BufferType
     *
     * @return preferred type of Buffer
     */
    @Override
    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    /**
     * Check whether the given BufferType is supported
     *
     * @param bufferType type of buffer to check support of
     * @return true of {@code bufferType} is supported, false otherwise
     */
    @Override
    public boolean supports(BufferType bufferType)
    {
        return bufferType == BufferType.OFF_HEAP;
    }

    /**
     * Lists the supported options by this compressor
     *
     * @return set of supported options
     */
    @Override
    public Set<String> supportedOptions()
    {
        return new HashSet<>(Collections.singletonList(COMPRESSION_LEVEL_OPTION_NAME));
    }


    @VisibleForTesting
    public int getCompressionLevel()
    {
        return compressionLevel;
    }

    @Override
    public Set<Uses> recommendedUses()
    {
        return recommendedUses;
    }
}
