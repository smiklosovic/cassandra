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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.compress.IDictionaryLoader.ZstdDictionaryInfo;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;

public class ZstdDictionaryCompressor extends AbstractZstdCompressor
{
    private static final Logger logger = LoggerFactory.getLogger(ZstdCompressor.class);

    private static final ConcurrentHashMap<Integer, ZstdDictionaryCompressor> instances = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Descriptor, ZstdDictionaryTrainer> trainers = new ConcurrentHashMap<>();

    public static final String DICTIONARY_ALLOCATED_SIZE_OPTION_NAME = "allocatedSize";
    public static final String DICTIONARY_DICT_SIZE_OPTION_NAME = "dictionarySize";

    public static final int DICTIONARY_DEFAULT_ALLOCATED_SIZE = 1024;
    public static final int DICTIONARY_DEFAULT_DICTIONARY_SIZE = 1024 * 1024;

    private final int allocatedSize;
    private final int dictSize;

    /**
     * Create a Zstd compressor with the given options
     *
     * @param options compressor parameters
     * @return compressor instance
     */
    public static ZstdDictionaryCompressor create(Map<String, String> options)
    {
        int level = getOrDefaultCompressionLevel(options);
        int allocatedSize = parseInt(options, DICTIONARY_ALLOCATED_SIZE_OPTION_NAME, DICTIONARY_DEFAULT_ALLOCATED_SIZE);
        int dictSize = parseInt(options, DICTIONARY_DICT_SIZE_OPTION_NAME, DICTIONARY_DEFAULT_DICTIONARY_SIZE);
        return getOrCreate(level, allocatedSize, dictSize);
    }

    /**
     * Private constructor
     *
     * @param compressionLevel level of compression
     * @param allocatedSize    allocated size of dictionary
     * @param dictSize         maximum size of dictionary
     */
    private ZstdDictionaryCompressor(int compressionLevel, int allocatedSize, int dictSize)
    {
        super(compressionLevel, ImmutableSet.of(Uses.GENERAL));
        this.allocatedSize = allocatedSize;
        this.dictSize = dictSize;
        logger.trace("Creating ZstdDictionaryCompressor with compression level={}, allocatedSize={}, dictSize={}",
                     compressionLevel, allocatedSize, dictSize);
    }

    private static int parseInt(Map<String, String> options, String key, int defaultValue)
    {
        if (options == null)
            return defaultValue;

        String val = options.get(key);

        if (val == null)
            return defaultValue;

        return Integer.parseInt(val);
    }

    public static ZstdDictionaryCompressor getOrCreate(int level, int allocatedSize, int dictSize)
    {
        return instances.computeIfAbsent(level, l -> new ZstdDictionaryCompressor(l, allocatedSize, dictSize));
    }

    @Override
    public boolean supportsDictionaryTraining()
    {
        return true;
    }

    @Override
    public ZstdDictionaryTrainer getDictionaryTrainer(Descriptor descriptor)
    {
        return trainers.computeIfAbsent(descriptor, d -> {
            ZstdDictionaryTrainer trainer = ZstdDictionaryTrainer.load(descriptor, compressionLevel);
            if (trainer == null)
            {
                logger.info("Loaded trainer from scratch");
                trainer = new ZstdDictionaryTrainer(allocatedSize, dictSize, compressionLevel);
            }
            else
            {
                logger.info("Loaded trainer for descriptor {} from disk", descriptor.toString());
            }
            return trainer;
        });
    }

    @Override
    public void removeDictionaryTrainer(Descriptor descriptor)
    {
        trainers.remove(descriptor);
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        throw new UnsupportedEncodingException("This compressor needs descriptor to operate on!");
    }

    @Override
    public int uncompress(Descriptor descriptor, byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        ZstdDictDecompress dictDecompress = getDictionaryTrainer(descriptor).dictDecompress;

        long dsz;

        if (dictDecompress != null)
        {
            logger.info("decompressing with dictionary!");
            dsz = Zstd.decompressFastDict(output,
                                          outputOffset,
                                          input,
                                          inputOffset,
                                          inputLength,
                                          dictDecompress);
        }
        else
        {
            logger.info("decompressing without dictionary!");
            dsz = Zstd.decompressByteArray(output,
                                           outputOffset,
                                           output.length - outputOffset,
                                           input,
                                           inputOffset,
                                           inputLength);
        }

        if (Zstd.isError(dsz))
            throw new IOException(String.format("Decompression failed due to %s", Zstd.getErrorName(dsz)));

        return (int) dsz;
    }

    @Override
    public void uncompress(Descriptor descriptor, ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            ZstdDictDecompress dictDecompress = getDictionaryTrainer(descriptor).dictDecompress;
            if (dictDecompress != null)
            {
                logger.info("decompressing with dictionary!");
                Zstd.decompress(output, input, dictDecompress);
            }
            else
            {
                logger.info("decompressing without dictionary!");
                Zstd.decompress(output, input);
            }
        }
        catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
        }
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        throw new UnsupportedEncodingException("This compressor needs descriptor to operate on!");
    }

    @Override
    public void compress(Descriptor descriptor, ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            ZstdDictCompress dictCompress = getDictionaryTrainer(descriptor).dictCompress;
            if (dictCompress != null)
            {
                logger.info("compressing with dictionary!");
                Zstd.compress(output, input, dictCompress);
            }
            else
            {
                logger.info("compressing without dictionary!");
                Zstd.compress(output, input);
            }
        }
        catch (Exception e)
        {
            throw new IOException("Compression failed", e);
        }
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        throw new UnsupportedEncodingException("This compressor needs descriptor to operate on!");
    }

    static class ZstdDictionaryLoader implements IDictionaryLoader
    {
        @Override
        public ZstdDictionaryInfo load(Descriptor descriptor, int compressionLevel)
        {
            File dataFile = new File(descriptor.filenameFor(Component.DATA));
            if (!dataFile.exists())
            {
                return null;
            }

            try
            {
                if (!new File(descriptor.filenameFor(Component.COMPRESSION_INFO)).exists())
                {
                    return null;
                }
                CompressionMetadata compressionMetadata = new CompressionMetadata(descriptor, dataFile.length());
                ByteBuffer dictionary = compressionMetadata.dictionary();
                return new ZstdDictionaryInfo(compressionLevel, dictionary);
            }
            catch (Exception ex)
            {
                throw new IllegalStateException("Unable to deserialize CompressionInfo!", ex);
            }
        }
    }

    public static class ZstdDictionaryTrainer implements IDictionaryTrainer
    {
        private ZstdDictTrainer trainer;
        private final int level;
        private ByteBuffer dictionary;
        private ZstdDictCompress dictCompress;
        private ZstdDictDecompress dictDecompress;

        public ZstdDictionaryTrainer(ZstdDictionaryInfo info)
        {
            this.level = info.compressionLevel;
            this.dictionary = info.dictionary;
            dictCompress = getDictCompress(dictionary, level);
            dictDecompress = getDictDecompress(dictionary);
        }

        public ZstdDictionaryTrainer(int allocatedSize, int dictSize, int level)
        {
            trainer = new ZstdDictTrainer(allocatedSize, dictSize);
            this.level = level;
        }

        public static ZstdDictionaryTrainer load(Descriptor descriptor, int compressionLevel)
        {
            ZstdDictionaryInfo load = new ZstdDictionaryLoader().load(descriptor, compressionLevel);
            if (load == null)
            {
                logger.info("Not loaded from disk!");
                return null;
            }
            return new ZstdDictionaryTrainer(load);
        }

        @Override
        public boolean isTrained()
        {
            return dictionary != null;
        }

        @Override
        public ByteBuffer getDictionary()
        {
            return dictionary;
        }

        private ZstdDictCompress getDictCompress(ByteBuffer buffer, int level)
        {
            return buffer == null ? null : new ZstdDictCompress(buffer.array(), level);
        }

        private ZstdDictDecompress getDictDecompress(ByteBuffer buffer)
        {
            return buffer == null ? null : new ZstdDictDecompress(buffer.array());
        }

        @Override
        public void trainDictionary()
        {
            if (dictionary == null)
            {
                try
                {
                    dictionary = ByteBuffer.wrap(trainer.trainSamples());
                    dictCompress = getDictCompress(dictionary, level);
                    dictDecompress = getDictDecompress(dictionary);
                    logger.info("Trained dictionary with size {}", dictionary.array().length);
                }
                catch (Exception ex)
                {
                    logger.warn("Not trained as there is not enough data!", ex);
                }
            }
        }

        @Override
        public void addSample(byte[] sample)
        {
            trainer.addSample(sample);
        }

        public int count = 0;

        @Override
        public void addSample(Partition partition)
        {
            if (dictionary != null)
                return;

            boolean shouldContinue = true;
            try (UnfilteredRowIterator unfilteredRowIterator = partition.unfilteredIterator())
            {
                while (unfilteredRowIterator.hasNext())
                {
                    Unfiltered next = unfilteredRowIterator.next();
                    if (next.isRow())
                    {
                        Row row = (Row) next;

                        for (org.apache.cassandra.schema.ColumnMetadata cd : row.columns())
                        {
                            org.apache.cassandra.db.rows.Cell<?> cell = row.getCell(cd);
                            String valueString = cd.type.getSerializer().deserialize(cell.buffer()).toString();
                            byte[] result = valueString.getBytes(StandardCharsets.UTF_8);
                            shouldContinue = trainer.addSample(result);
                            if (!shouldContinue)
                                break;
                        }
                    }
                    if (!shouldContinue)
                        break;
                }
            }
        }
    }
}
