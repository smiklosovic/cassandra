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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.Zstd;

/**
 * ZSTD Compressor
 */
public class ZstdCompressor extends AbstractZstdCompressor
{
    private static final Logger logger = LoggerFactory.getLogger(ZstdCompressor.class);

    private static final ConcurrentHashMap<Integer, ZstdCompressor> instances = new ConcurrentHashMap<>();

    /**
     * Create a Zstd compressor with the given options
     *
     * @param options compressor parameters
     * @return compressor instance
     */
    public static ZstdCompressor create(Map<String, String> options)
    {
        int level = getOrDefaultCompressionLevel(options);

        if (!isValid(level))
            throw new IllegalArgumentException(String.format("%s=%d is invalid", COMPRESSION_LEVEL_OPTION_NAME, level));

        return getOrCreate(level);
    }

    /**
     * Private constructor
     *
     * @param compressionLevel level of compression
     */
    private ZstdCompressor(int compressionLevel)
    {
        super(compressionLevel, ImmutableSet.of(Uses.GENERAL));
        logger.trace("Creating Zstd Compressor with compression level={}", compressionLevel);
    }

    public static ZstdCompressor getOrCreate(int level)
    {
        return instances.computeIfAbsent(level, l -> new ZstdCompressor(level));
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
    throws IOException
    {
        long dsz = Zstd.decompressByteArray(output, outputOffset, output.length - outputOffset,
                                            input, inputOffset, inputLength);

        if (Zstd.isError(dsz))
            throw new IOException(String.format("Decompression failed due to %s", Zstd.getErrorName(dsz)));

        return (int) dsz;
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            Zstd.decompress(output, input);
        } catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
        }
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            Zstd.compress(output, input, compressionLevel, ENABLE_CHECKSUM_FLAG);
        } catch (Exception e)
        {
            throw new IOException("Compression failed", e);
        }
    }
}
