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
 * This example shows how to use Carrot Set to keep list of all English words
 *
 * <p>File: words_alpha.txt.s.
 *
 * <p>RESULTS: 0. Total number of words is 370099 1. Raw size of all words is 3,494,665 bytes 2.
 * Carrot NoCompression - RAM usage 4,306,191, COMPRESSION = 0.81 3 Carrot LZ4 compression - RAM
 * usage 2,857,311, COMPRESSION = 1.22 4. Carrot LZ4HC compression - RAM usage 2,601,695,
 * COMPRESSION = 1.34
 *
 * <p>LZ4 compression relative to NoCompression = 1.22/0.81 = 1.5 LZ4HC compression relative to
 * NoCompression = 1.34/0.81 = 1.65
 *
 * <p>Redis SET estimated RAM usage is 35MB ( ~ 100 bytes per word) (actually it can be more, this
 * is a low estimate based on evaluating Redis code)
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>1) No compression 35M/3.5M ~ 10x 2) LZ4 compression 35M/2.8M ~ 15x 3) LZ4HC compression
 * 35M/2.6M ~ 16.5x
 *
 * <p>Effect of a compression:
 *
 * <p>LZ4 - 1.22/0.81 = 1.5 (to no compression) LZ4HC - 1.34/0.81 = 1.65 (to no compression)
 */
public class SetsAllEnglishWords {

  private static final Logger log = LogManager.getLogger(SetsAllEnglishWords.class);

  static {
    UnsafeAccess.debug = true;
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
        log.debug("Loaded {}", +count);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug(
        "Loaded {} words, total size={} in {}ms. RAM usage={}",
        count * keys.size(),
        totalLength,
        endTime - startTime,
        UnsafeAccess.getAllocatedMemory());
    log.debug("COMPRESSION={}", (double) totalLength / UnsafeAccess.getAllocatedMemory());
    dis.close();

    BigSortedMap.printGlobalMemoryAllocationStats();
    for (Key key : keys) {
      Sets.DELETE(map, key.address, key.length);
    }
    map.dispose();
  }

  private static void usage() {
    log.fatal("usage: java org.bigbase.carrot.examples.AllEnglishWords domain_list_file");
    System.exit(-1);
  }
}
