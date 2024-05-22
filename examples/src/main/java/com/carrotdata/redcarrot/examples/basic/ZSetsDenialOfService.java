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
package com.carrotdata.redcarrot.examples.basic;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.ops.OperationFailedException;
import com.carrotdata.redcarrot.redis.RedisConf;
import com.carrotdata.redcarrot.redis.zsets.ZSets;
import com.carrotdata.redcarrot.util.UnsafeAccess;

/**
 * This example shows how to use Carrot ZSets to store
 *
 * <p>{host_name, number_of_requests} pairs in a sorted sets to track most offensive sites (for TopN
 * queries). We track information on each host and number of requests for a particular resource
 * which came from this host.
 *
 * <p>Test description: <br>
 * 1. Load 1M {host, number_of_requests} pairs into Carrot sorted set 2. Calculate RAM usage
 *
 * <p>number_of_requests is random number between 1 and 1000 heavily skewed towards 0 host -
 * synthetic (xx.xx.xx.xx) string or a real host name from a provided file
 *
 * <p>Results: 0. Average data size {host, number_of_requests} = 21 bytes 1. No compression. Used
 * RAM per session object is 53 bytes 2. LZ4 compression. Used RAM per session object is 34 bytes 3.
 * LZ4HC compression. Used RAM per session object is 30 bytes, ZSTD - 18 bytes
 *
 * <p>Redis usage per record , using ZSets is 112
 *
 * <p>Total used Redis memory is shown by 'used_memory' in Redis CLI.
 *
 * <p>RAM usage (Redis-to-Carrot)
 *
 * <p>
 * 1) No compression 112/53 ~ 2.1x 
 * 2) LZ4 compression 112/34 ~ 3.3x 
 * 3) LZ4HC compression 112/30 = 3.8x, 
 * 4) ZSTD = 112 / 18 ~ 6.4
 *
 * <p>Effect of a compression:
 *
 * <p>LZ4 - 53/34 ~ 1.6x (to no compression) LZ4HC - 53/30 ~ 1.8x (to no compression), ZSTD = 53 / 18 =~ 3.0x
 */
public class ZSetsDenialOfService {

  private static final Logger log = LogManager.getLogger(ZSetsDenialOfService.class);

  static {
    //UnsafeAccess.debug = true;
  }

  static long buffer = UnsafeAccess.malloc(4096);

  static long fieldBuf = UnsafeAccess.malloc(64);

  static long N = 1000000;
  /*
   * We use low maximum request count
   * to reflect the fact that majority of hosts are legitimate
   * and do not overwhelm the service wit a bogus requests.
   */
  static double MAX = 1000d;

  static long totalDataSize = 0;
  static Random rnd = new Random();
  static int index = 0;

  static List<String> hosts = new ArrayList<String>();

  public static void main(String[] args) throws IOException, OperationFailedException {
    RedisConf conf = RedisConf.getInstance();
    conf.setTestMode(true);
//    log.debug("RUN compression = NONE");
//    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
//    runTest();
//    log.debug("RUN compression = LZ4");
//    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
//    runTest();
    log.debug("RUN compression = ZSTD");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
    runTest();

    // Now load hosts
    if (args.length > 0) {
      loadHosts(args[0]);
      log.debug("RUN compression = NONE - REAL DATA");
      BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
      runTest();
      log.debug("RUN compression = LZ4 - REAL DATA");
      BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
      runTest();
      log.debug("RUN compression = ZSTD - REAL DATA");
      BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.ZSTD));
      runTest();
    }
  }

  @SuppressWarnings("deprecation")
  private static void loadHosts(String fileName) throws IOException {
    File f = new File(fileName);
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    String line = null;

    while ((line = dis.readLine()) != null) {
      hosts.add(line);
    }
    dis.close();
  }

  /**
   * This test runs pure synthetic data: host names are random IPv4 addresses, request numbers are
   * uniformly distributed between 0 and 100000
   *
   * @throws IOException
   * @throws OperationFailedException
   */
  private static void runTest() throws IOException, OperationFailedException {

    index = 0;

    BigSortedMap map = new BigSortedMap(1000000000);

    totalDataSize = 0;

    long startTime = System.currentTimeMillis();
    String key = "key";
    long keyPtr = UnsafeAccess.malloc(key.length());
    int keySize = key.length();

    UnsafeAccess.copy(key.getBytes(), 0, keyPtr, keySize);

    double[] scores = new double[1];
    long[] memPtrs = new long[1];
    int[] memSizes = new int[1];
    long max = hosts.size() > 0 ? hosts.size() : N;
    for (int i = 0; i < max; i++) {

      String host = getNextHost();
      double score = getNextScore();
      int fSize = host.length();
      UnsafeAccess.copy(host.getBytes(), 0, fieldBuf, fSize);

      scores[0] = score;
      memPtrs[0] = fieldBuf;
      memSizes[0] = fSize;
      totalDataSize += fSize + 8 /*SCORE size*/;

      ZSets.ZADD(map, keyPtr, keySize, scores, memPtrs, memSizes, false);

      if (i % 10000 == 0 && i > 0) {
        log.debug("zset {}", i);
      }
    }
    long endTime = System.currentTimeMillis();

    long num = hosts.size() > 0 ? hosts.size() : N;

    log.debug(
        "Loaded {} [host, number] pairs, total size={} in {}ms. RAM usage={} RAM per record={} COMPRESSION={}",
        num,
        totalDataSize,
        endTime - startTime,
        UnsafeAccess.getAllocatedMemory(),
        (double) UnsafeAccess.getAllocatedMemory() / num,
        (double) totalDataSize / UnsafeAccess.getAllocatedMemory());

    BigSortedMap.printGlobalMemoryAllocationStats();
    map.dispose();
  }

  static double getNextScore() {
    double d = rnd.nextDouble();
    return Math.rint(Math.pow(d, 10) * MAX);
  }

  static String getNextHost() {
    if (hosts.size() > 0) {
      return hosts.get(index++);
    }

    return Integer.toString(rnd.nextInt(256))
        + "."
        + Integer.toString(rnd.nextInt(256))
        + "."
        + Integer.toString(rnd.nextInt(256))
        + "."
        + Integer.toString(rnd.nextInt(256));
  }
}
