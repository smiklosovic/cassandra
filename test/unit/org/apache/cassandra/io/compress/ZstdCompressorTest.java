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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;

import static org.junit.Assert.assertEquals;

/**
 * Zstd Compressor specific tests. General compressor tests are in {@link CompressorTest}
 */
public class ZstdCompressorTest
{
    @Test
    public void emptyConfigurationUsesDefaultCompressionLevel()
    {
        ZstdCompressor compressor = ZstdCompressor.create(Collections.emptyMap());
        assertEquals(ZstdCompressor.DEFAULT_COMPRESSION_LEVEL, compressor.getCompressionLevel());
    }

    @Test(expected = IllegalArgumentException.class)
    public void badCompressionLevelParamThrowsExceptionMin()
    {
        ZstdCompressor.create(ImmutableMap.of(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(Zstd.minCompressionLevel() - 1)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void badCompressionLevelParamThrowsExceptionMax()
    {
        ZstdCompressor.create(ImmutableMap.of(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(Zstd.maxCompressionLevel() + 1)));
    }

    @Test
    public void testDict() throws Exception
    {
        String jsonString1 = "very long hello world string 1";
        String jsonString2 = "very long hello world string 2";
        String jsonString3 = "very long hello world string 3";

        // 1 kb of samples

        int allSamples = 0;
        int countOfSamples = 0;

        ZstdDictTrainer trainer = new ZstdDictTrainer(1024 * 1024, 1024 * 1024);
        for (int j = 0; j < 1024 * 1024; j++)
        {
            byte[] sample = ("very long hello world string " + j).getBytes(StandardCharsets.UTF_8);
            if (trainer.addSample(sample))
            {
                allSamples += sample.length;
                countOfSamples++;
            }
            else
                break;
        }
        byte[] dict_buff = trainer.trainSamples();

        System.out.println("dict length " + dict_buff.length);
        System.out.println("all samples length " + allSamples);
        System.out.println("count of samples " + countOfSamples);

        String jsonString = "very long hello world string";

        byte[] json = jsonString.getBytes(StandardCharsets.UTF_8);
        ZstdDictCompress zstdDictCompress = new ZstdDictCompress(dict_buff, Zstd.defaultCompressionLevel());
        byte[] compressed = Zstd.compress(json, zstdDictCompress);

        // Tricky moment, you have to pass json full length to decompress method
        int jsonFullLength = json.length;

        // Decompress
        ZstdDictDecompress zstdDictDecompress = new ZstdDictDecompress(dict_buff);
        byte[] decompressed = Zstd.decompress(compressed, zstdDictDecompress, jsonFullLength);
        String jsonStringResult = new String(decompressed, StandardCharsets.UTF_8);

        System.out.println(jsonString);
        System.out.println(jsonStringResult);
        System.out.println(compressed.length);
        System.out.println(decompressed.length);
    }
}
