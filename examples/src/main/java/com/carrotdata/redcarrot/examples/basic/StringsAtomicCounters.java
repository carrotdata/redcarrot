/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.examples.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.ops.OperationFailedException;
import com.carrotdata.redcarrot.redis.RedisConf;
import com.carrotdata.redcarrot.redis.strings.Strings;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;

/**
 * This example shows how to use Carrot Strings.INCRBY and Strings.INCRBYFLOAT to keep huge list of
 * atomic counters Test Description:
 * <p>
 * Key format: "counter:number" number = [0:1M]
 * <p>
 * 1. Load 1M long and double counters 2. Increment each by random number 0 - 1000 3. Calculate
 * Memory usage
 * <p>
 * Results:
 * <p>
 * 1. Average counter size is 21 (13 bytes - key, 8 - value) 2. Carrot No compression. 37.5 bytes (
 * 8 bytes expiration + 2 bytes for key and value length + 1 byte for data type) per counter 3.
 * Carrot LZ4 - 10.8 bytes per counter 4. Carrot LZ4HC - 10.3 bytes per counter 5. Carrot ZSTD - 7.2
 * bytes 6. Redis memory usage per counter is 57.5 bytes
 * <p>
 * RAM usage (Redis-to-Carrot)
 * <p>
 * 1) No compression 57.5/37.5 ~ 1.5x 2) LZ4 compression 57.5/10.8 ~ 5.3x 3) LZ4HC compression
 * 57.5/10.3 ~ 5.6x 4) ZSTD compression 57.5 / 7.2 ~ 8.0
 * <p>
 * Effect of a compression:
 * <p>
 * LZ4 - 37.5/10.8 = 3.5 (to no compression) LZ4HC - 37.5/10.3 = 3.6 (to no compression) ZSTD - 37.5
 * / 7.2 = 5.2
 * <p>
 * Redis
 * <p>
 * In Redis Hashes with ziplist encodings can be used to keep counters TODO: we need to compare
 * Redis optimized version with our default
 */
public class StringsAtomicCounters {

  private static final Logger log = LogManager.getLogger(StringsAtomicCounters.class);

  static {
    // UnsafeAccess.debug = true;
  }

  static long buffer = UnsafeAccess.malloc(4096);
  static List<Key> keys = new ArrayList<Key>();
  static long keyTotalSize = 0;
  static long N = 1000000;
  static int MAX_VALUE = 1000;

  static {
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;
      byte[] bkey = skey.getBytes();
      long key = UnsafeAccess.malloc(bkey.length);
      UnsafeAccess.copy(bkey, 0, key, bkey.length);
      keys.add(new Key(key, bkey.length));
      keyTotalSize += skey.length();
    }
    Collections.shuffle(keys);
  }

  public static void main(String[] args) throws IOException, OperationFailedException {

    RedisConf conf = RedisConf.getInstance();
    conf.setTestMode(true);
    log.debug("RUN compression = NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest();
    log.debug("RUN compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest();
    log.debug("RUN compression = LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest();
    log.debug("RUN compression = ZSTD");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    runTest();
  }

  private static void runTest() throws IOException, OperationFailedException {

    BigSortedMap map = new BigSortedMap(100000000);

    long startTime = System.currentTimeMillis();
    int count = 0;
    Random r = new Random();
    for (Key key : keys) {
      count++;
      Strings.INCRBY(map, key.address, key.length, r.nextInt(MAX_VALUE));
      if (count % 100000 == 0) {
        log.debug("set long {}", count);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("Loaded {} long counters of avg size={} each in {}ms. RAM usage={}", keys.size(),
      keyTotalSize / N + 8, endTime - startTime, UnsafeAccess.getAllocatedMemory() - keyTotalSize);

    BigSortedMap.printGlobalMemoryAllocationStats();
    // Delete all
    for (Key key : keys) {
      Strings.DELETE(map, key.address, key.length);
    }

    // // Now test doubles
    // count = 0;
    // startTime = System.currentTimeMillis();
    //
    // for (Key key: keys) {
    // count++;
    // Strings.INCRBYFLOAT(map, key.address, key.length, 1d);
    // if (count % 100000 == 0) {
    // log.debug("set float {}", count);
    // }
    // }
    //
    // endTime = System.currentTimeMillis();
    //
    // log.debug(
    // "Loaded {} double counters of avg size={} each in {}ms. RAM usage={}",
    // keys.size(),
    // +(keyTotalSize / N + 8),
    // endTime - startTime,
    // UnsafeAccess.getAllocatedMemory() - keyTotalSize);
    BigSortedMap.printGlobalMemoryAllocationStats();

    map.dispose();
  }
}
