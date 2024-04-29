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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
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
 * <p>1. Raw size of all data is 7,200,570 bytes 2. Carrot NoCompression - RAM usage 8,566,630,
 * COMPRESSION = 0.84 3 Carrot LZ4 compression - RAM usage 6,657,078, COMPRESSION = 1.08 4. Carrot
 * LZ4HC compression - RAM usage 6,154,038, COMPRESSION = 1.17
 *
 * <p>LZ4 compression relative to NoCompression = 1.08/0.84 = 1.29 LZ4HC compression relative to
 * NoCompression = 1.17/0.84 = 1.4
 *
 * <p>Redis SET estimated RAM usage is 50MB (~ 100 bytes per record) (actually it can be more, this
 * is a low estimate based on evaluating Redis code)
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>1) No compression 50M/8.5M ~ 5.9x 2) LZ4 compression 50M/6.6M ~ 7.6x 3) LZ4HC compression
 * 50M/6.1M ~ 8.2x
 *
 * <p>Effect of a compression:
 *
 * <p>LZ4 - 1.08/0.84 = 1.29 (to no compression) LZ4HC - 1.17/0.84 = 1.4 (to no compression)
 *
 * <p>Notes: Redis can store these URLS in a Hash with ziplist encoding using HSET ( key, field =
 * URL, "1")
 *
 * <p>Avg. URL size is 14. ziplist overhead for {URL, "1"} pair is 4 = 2+2. So usage is going to be
 * 18 per URL
 *
 * <p>LZ4HC compression = 12 bytes per URL. So, we have at least 1.5x advantage even for this Redis
 * hack
 *
 * <p>Notes: Using Hash hack to store large set of objects has one downside - you can't use SET
 * specific API: union, intersect etc.
 */
public class SetsSpamFilter {

  private static final Logger log = LogManager.getLogger(SetsSpamFilter.class);

  static {
    UnsafeAccess.debug = true;
  }

  static long buffer = UnsafeAccess.malloc(4096);
  static List<Key> keys = new ArrayList<Key>();

  static {
    Random r = new Random();
    byte[] bkey = new byte[8];
    for (int i = 0; i < 10; i++) {
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
    log.debug("RUN compression = NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest(args[0]);
    log.debug("RUN compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest(args[0]);
    log.debug("RUN compression = LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest(args[0]);
  }

  @SuppressWarnings("deprecation")
  private static void runTest(String fileName) throws IOException {

    BigSortedMap map = new BigSortedMap(100000000);
    File f = new File(fileName);
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    String line = null;
    long totalLength = 0;
    int count = 0;

    long startTime = System.currentTimeMillis();
    while ((line = dis.readLine()) != null) {
      byte[] data = line.getBytes();
      UnsafeAccess.copy(data, 0, buffer, data.length);
      totalLength += data.length * keys.size();
      count++;
      for (Key key : keys) {
        Sets.SADD(map, key.address, key.length, buffer, data.length);
      }
      if ((count % 100000) == 0 && count > 0) {
        log.debug("Loaded {}", count);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug(
        "Loaded {} records, total size={} in {}ms. RAM usage={}",
        count * keys.size(),
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
