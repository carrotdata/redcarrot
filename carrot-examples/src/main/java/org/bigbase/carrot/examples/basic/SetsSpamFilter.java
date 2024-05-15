/*
 Copyright (C) 2021-present Carrot, Inc.

 <p>This program is free software: you can redistribute it and/or modify it under the terms of the
 Server Side Public License, version 1, as published by MongoDB, Inc.

 <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 Server Side Public License for more details.

 <p>You should have received a copy of the Server Side Public License along with this program. If
 not, see <http://www.mongodb.com/licensing/server-side-public-license>.
*/
package org.bigbase.carrot.examples.basic;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.RedisConf;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * This example shows how to use Carrot Set to keep list of a REAL spam domains
 *
 * <p>File: spam_domains.txt.s
 *
 * <p>Test loads list of real spam domains (51672 records) into 10 different sets, total number of
 * loaded records is 516720 (51672 * 10)
 *
 * <p>RESULTS:
 *
 * <p>
 * 1. Raw size of all data is 7,200,570 bytes 
 * 2. Carrot NoCompression - RAM usage 8,566,630, COMPRESSION = 0.84 
 * 3. Carrot LZ4 compression - RAM usage 6,657,078, COMPRESSION = 1.08 
 * 4. Carrot LZ4HC compression - RAM usage 6,154,038, COMPRESSION = 1.17,
 * 5. ZSTD compression - RAM usage 3804646, compression 2.1 (dictionary size 256KB, level = 3)
 *
 * <p>
 * LZ4 compression relative to NoCompression = 1.08/0.84 = 1.29 
 * LZ4HC compression relative to NoCompression = 1.17/0.84 = 1.4
 * ZSTD compression relative to NoCompression = 2.1/0.84 = 2.5
 *
 * <p>Redis 7.2.4 SET RAM usage is ~22MiB (~ 44 bytes per record) 
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>
 * 1) No compression 22M/8.5M ~ 2.6x 
 * 2) LZ4 compression 22M/6.6M ~ 3.3x 
 * 4) ZSTD compression = 22M/3.8 > 5.8x
 *
 * 
 * <p>Notes: Redis can store these URLS in a Hash with ziplist encoding using HSET ( key, field =
 * URL, "1") - How?
 *
 * <p>Avg. URL size is 14. ziplist overhead for {URL, "1"} pair is 4 = 2+2. So usage is going to be
 * 18 per URL ( ACtually ~= 19)
 *
 * <p>ZSTD compression = 7.4 bytes per URL. So, we have at least ~ 2.6x advantage even for this Redis
 * hack
 *
 * <p>Notes: Using Hash hack to store large set of objects has several downside:
 * 1. You can't use SET specific API: union, intersect etc.
 * 2. Your data set must be more static, otherwise you won't be able to determine optimal number of hash keys
 *     to use to keep number of members below maximum ziplist value. 
 */
public class SetsSpamFilter {

  private static final Logger log = LogManager.getLogger(SetsSpamFilter.class);

  static {
    //UnsafeAccess.debug = true;
  }

  static long buffer = UnsafeAccess.malloc(4096);
  static List<Key> keys = new ArrayList<Key>();

  static {
    Random r = new Random();
    byte[] bkey = new byte[8];
    for (int i = 0; i < 100; i++) {
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
//    log.debug("RUN compression = NONE");
//    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
//    runTest(args[0]);
//    log.debug("RUN compression = LZ4");
//    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
//    runTest(args[0]);
    log.debug("RUN compression = ZSTD");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    runTest(args[0]);
  }

  private static void runTest(String fileName) throws IOException {

    BigSortedMap map = new BigSortedMap(100000000);
    File f = new File(fileName);
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    long totalLength = 0;
    int count = 0;
    List<String> lines = Files.readAllLines(Path.of(fileName));
    long startTime = System.currentTimeMillis();
    for (Key key : keys) {
    for (String line: lines) {
      byte[] data = line.getBytes();
      UnsafeAccess.copy(data, 0, buffer, data.length);
      totalLength += data.length;
      count++;
      Sets.SADD(map, key.address, key.length, buffer, data.length);
      if ((count % 100000) == 0 && count > 0) {
        log.debug("Loaded {}", count);
      }
    }
  }
    long endTime = System.currentTimeMillis();

    log.debug(
        "Loaded {} records, total size={} in {}ms. RAM usage={}",
        count,
        totalLength,
        endTime - startTime,
        UnsafeAccess.getAllocatedMemory());
    log.debug("COMPRESSION ={}", (double) totalLength / UnsafeAccess.getAllocatedMemory());
    dis.close();

    BigSortedMap.printGlobalMemoryAllocationStats();
    for (Key key : keys) {
      Sets.DELETE(map, key.address, key.length);
    }
    map.dispose();
  }

  private static void usage() {
    log.fatal("usage: java org.bigbase.carrot.examples.SpamFilter domain_list_file");
    System.exit(-1);
  }
}
