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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class ZstdDictionaryCompressor extends AbstractZstdCompressor
{
    private static final Logger logger = LoggerFactory.getLogger(ZstdCompressor.class);

    private static final ConcurrentHashMap<Pair<Integer, File>, ZstdDictionaryCompressor> instances = new ConcurrentHashMap<>();

    @VisibleForTesting
    public static final String DICTIONARY_OPTION_NAME = "dictionary";

    private final File dictionary;
    private ZstdDictCompress zstdDictCompress;
    private ZstdDictDecompress zstdDictDecompress;

    /**
     * Create a Zstd compressor with the given options
     *
     * @param options compressor parameters
     * @return compressor instance
     */
    public static ZstdDictionaryCompressor create(Map<String, String> options)
    {
        int level = getOrDefaultCompressionLevel(options);
        File dictionary = getDictionary(options);

        if (!isValid(level))
            throw new IllegalArgumentException(String.format("%s=%d is invalid", COMPRESSION_LEVEL_OPTION_NAME, level));
        if (dictionary == null)
            throw new IllegalArgumentException("There is not dictionary specified!");
        if (!dictionary.exists())
            throw new IllegalArgumentException(String.format("Dictionary %s does not exist.", dictionary));
        if (!dictionary.isFile())
            throw new IllegalArgumentException(String.format("Dictionary %s is not file!", dictionary));
        if (!dictionary.isReadable())
            throw new IllegalArgumentException(String.format("Dictionary %s is not readable!", dictionary));

        return getOrCreate(level, dictionary);
    }

    /**
     * Private constructor
     *
     * @param compressionLevel level of compression
     * @param dictionary       trained dictionary
     */
    private ZstdDictionaryCompressor(int compressionLevel, File dictionary)
    {
        super(compressionLevel, ImmutableSet.of(Uses.GENERAL));
        this.dictionary = dictionary;
        logger.trace("Creating ZstdDictionaryCompressor with compression level={} and dictionary={}",
                     compressionLevel, dictionary);
    }

    private static File getDictionary(Map<String, String> options)
    {
        if (options == null)
            return null;

        String val = options.get(DICTIONARY_OPTION_NAME);

        if (val == null)
            return null;

        return new File(val);
    }

    public static ZstdDictionaryCompressor getOrCreate(int level, File dictionary)
    {
        return instances.computeIfAbsent(Pair.create(level, dictionary), key -> {
            ZstdDictionaryCompressor zstdDictionaryCompressor = new ZstdDictionaryCompressor(key.left, key.right);
            zstdDictionaryCompressor.init();
            return zstdDictionaryCompressor;
        });
    }

    void init()
    {
        logger.info("Initializing compressor for level {} for dictionary {}", compressionLevel, dictionary);

        try (DataInputStreamPlus in = new DataInputStreamPlus(new BufferedInputStream(new FileInputStream(dictionary.toJavaIOFile()))))
        {
            byte[] dict_buff = ByteBufferUtil.readBytes(in, (int) dictionary.length());
            zstdDictCompress = new ZstdDictCompress(dict_buff, compressionLevel);
            zstdDictDecompress = new ZstdDictDecompress(dict_buff);
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Unable to initialize ZstdDictionaryCompressor!", ex);
        }
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        long dsz = Zstd.decompressFastDict(output,
                                           outputOffset,
                                           input,
                                           inputOffset,
                                           inputLength,
                                           zstdDictDecompress);

        if (Zstd.isError(dsz))
            throw new IOException(String.format("Decompression failed due to %s", Zstd.getErrorName(dsz)));

        return (int) dsz;
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            Zstd.decompress(output, input, zstdDictDecompress);
        }
        catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
        }
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            Zstd.compress(output, input, zstdDictCompress);
        }
        catch (Exception e)
        {
            throw new IOException("Compression failed", e);
        }
    }
}
