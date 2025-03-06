/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.examples.basic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.redis.RedisConf;
import com.carrotdata.redcarrot.redis.sets.Sets;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;

/**
 * This example shows how to use Carrot Set to keep list of all English words
 * <p>
 * File: words_alpha.txt.s.
 * <p>
 * RESULTS: 0. Total number of words is 370099 1. Raw size of all words is 3,494,665 bytes 2. Carrot
 * NoCompression - RAM usage 4,286,464, COMPRESSION = 0.81 3. Carrot LZ4 compression - RAM usage
 * 2,866,432, COMPRESSION = 1.22 4. Carrot LZ4HC compression - RAM usage 2,601,695, COMPRESSION =
 * 1.34 5. Carrot ZSTD compression - RAM usage 1,918,592 COMPRESSION = 1.84
 * <p>
 * LZ4 compression relative to NoCompression = 1.22/0.81 = 1.5 LZ4HC compression relative to
 * NoCompression = 1.34/0.81 = 1.65 ZSTD compression relative to NoCompression = 1.84 / 0.81 = 2.27
 * <p>
 * Redis SET RAM usage is 13.75MB
 * <p>
 * RAM usage (Redis-to-Carrot)
 * <p>
 * 1) No compression 13.75M/4.3M ~ 3.2x 2) LZ4 compression 13.75M/2.8M ~ 4.9x 4) ZSTD compression
 * 13.75M/1.9M ~ 7.2x Redis Set ziplist encoding usage is ~4,800,000
 * <p>
 * RAM usage (Redis-to-Carrot)
 * <p>
 * 1) No compression 4.8M/4.3M ~ 1.1x 2) LZ4 compression 4.8M/2.8M ~ 1.7x 4) ZSTD compression
 * 4.8M/1.9M ~ 2.5x Note: splitting one large set into number of small sets to use memory efficient
 * encoding has two major drawbacks: 1. You want be able to perform some set-related operations,
 * such union, intersect etc 2. This technique is limited to static mostly sets because you have to
 * know in advance how many subsets you have to create to guarantee the optimal encoding in majority
 * of them.
 */
public class SetsAllEnglishWords {

  private static final Logger log = LogManager.getLogger(SetsAllEnglishWords.class);

  static {
    // UnsafeAccess.debug = true;
  }

  static long buffer = UnsafeAccess.malloc(4096);
  static List<Key> keys = new ArrayList<Key>();

  static {
    Random r = new Random();
    byte[] bkey = new byte[8];
    for (int i = 0; i < 1; i++) {
      r.nextBytes(bkey);
      long key = UnsafeAccess.malloc(bkey.length);
      UnsafeAccess.copy(bkey, 0, key, bkey.length);
      keys.add(new Key(key, bkey.length));
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      usage();
    }
    RedisConf conf = RedisConf.getInstance();
    conf.setTestMode(true);
    log.debug("RUN compression = NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest(args[0]);
    log.debug("RUN compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest(args[0]);
    log.debug("RUN compression = ZSTD");

    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    runTest(args[0]);
  }

  private static void runTest(String fileName) throws IOException {

    BigSortedMap map = new BigSortedMap(100000000);
    long totalLength = 0;
    int count = 0;
    List<String> lines = Files.readAllLines(Path.of(fileName));
    long startTime = System.currentTimeMillis();
    for (Key key : keys) {
      for (String line : lines) {
        byte[] data = line.getBytes();
        UnsafeAccess.copy(data, 0, buffer, data.length);
        totalLength += data.length;
        count++;
        Sets.SADD(map, key.address, key.length, buffer, data.length);
        if ((count % 100000) == 0 && count > 0) {
          log.debug("Loaded {}", +count);
        }
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("Loaded {} words, total size={} in {}ms. RAM usage={}", count, totalLength,
      endTime - startTime, UnsafeAccess.getAllocatedMemory());
    log.debug("COMPRESSION={}", (double) totalLength / UnsafeAccess.getAllocatedMemory());
    // dis.close();

    BigSortedMap.printGlobalMemoryAllocationStats();
    for (Key key : keys) {
      Sets.DELETE(map, key.address, key.length);
    }
    map.dispose();
  }

  private static void usage() {
    log.fatal("usage: java com.carrotdata.redcarrot.examples.AllEnglishWords domain_list_file");
    System.exit(-1);
  }
}
