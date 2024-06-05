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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.examples.util.UserSession;
import com.carrotdata.redcarrot.ops.OperationFailedException;

import redis.clients.jedis.Jedis;

/**
 * This example shows how to use Redis Strings to store user sessions objects
 * <p>
 * User Session structure: "SessionID" - A unique, universal identifier for the session data
 * structure (16 bytes). "Host" - host name or IP Address The location from which the client
 * (browser) is making the request. "UserID" - Set to the user's distinguished name (DN) or the
 * application's principal name. "Type" - USER or APPLICATION "State" - session state: VALID,
 * INVALID Defines whether the session is valid or invalid. "MaxIdleTime" - Maximum Idle Time
 * Maximum number of minutes without activity before the session will expire and the user must
 * reauthenticate. "MaxSessionTime" - Maximum Session Time. Maximum number of minutes (activity or
 * no activity) before the session expires and the user must reauthenticate. "MaxCachingTime" -
 * Maximum number of minutes before the client contacts OpenSSO Enterprise to refresh cached session
 * information.
 * <p>
 * Test description: <br>
 * UserSession object has 8 fields, one field (UserId) is used as a String key
 * <p>
 * Average key + session object size is 222 bytes. We load 100K user session objects
 * <p>
 * Results: 0. Average user session data size = 222 bytes (includes key size) 1. No compression.
 * Used RAM per session object is 275 bytes (COMPRESSION= 0.8) 2. LZ4 compression. Used RAM per
 * session object is 94 bytes (COMPRESSION = 2.37) 3. LZ4HC compression. Used RAM per session object
 * is 88 bytes (COMPRESSION = 2.5)
 * <p>
 * Redis usage per session object, using String encoding is ~300 bytes
 * <p>
 * RAM usage (Redis-to-Carrot)
 * <p>
 * 1) No compression 290/275 ~ 1.16x 2) LZ4 compression 290/94 ~ 3.4x 3) LZ4HC compression 290/88 ~
 * 3.64x
 * <p>
 * Effect of a compression:
 * <p>
 * LZ4 - 2.37/0.8 = 2.96 (to no compression) LZ4HC - 2.5/0.8 = 3.13 (to no compression)
 */
public class RedisStringsUserSessions {

  private static final Logger log = LogManager.getLogger(RedisStringsUserSessions.class);

  static long N = 1000000;
  static long totalDataSize = 0;
  static List<UserSession> userSessions = new ArrayList<UserSession>();
  static AtomicLong index = new AtomicLong(0);
  static int NUM_THREADS = 1;

  static {
    for (int i = 0; i < N; i++) {
      userSessions.add(UserSession.newSession(i));
    }
    Collections.shuffle(userSessions);
  }

  public static void main(String[] args) throws IOException, OperationFailedException {

    log.debug("Run Redis Cluster");
    Jedis client = getClient();
    client.flushAll();
    Runnable r = () -> {
      try {
        runLoad();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach(x -> x.start());
    Arrays.stream(workers).forEach(x -> {
      try {
        x.join();
      } catch (Exception e) {
      }
    });
    long end = System.currentTimeMillis();

    log.debug("Finished {} sets in {}ms. RPS={}", N, end - start, N * 1000 / (end - start));

    // index.set(0);
    //
    // r =
    // () -> {
    // try {
    // runGets();
    // } catch (Exception e) {
    // log.error("StackTrace: ", e);
    // }
    // };
    // workers = new Thread[NUM_THREADS];
    // for (int i = 0; i < NUM_THREADS; i++) {
    // workers[i] = new Thread(r);
    // }
    // start = System.currentTimeMillis();
    // Arrays.stream(workers).forEach(x -> x.start());
    // Arrays.stream(workers)
    // .forEach(
    // x -> {
    // try {
    // x.join();
    // } catch (Exception e) {
    // }
    // });
    // end = System.currentTimeMillis();
    //
    // log.debug("Finished {} sets in {}ms. RPS={}", N, end - start, N * 1000 / (end - start));

    start = System.currentTimeMillis();
    client.save();
    end = System.currentTimeMillis();
    log.debug("DB save took {}ms", end - start);
    client.close();
  }

  private static Jedis getClient() {
    return new Jedis("localhost");
  }

  private static int getIndexes(int[] to) {
    int count = 0;
    for (int i = 0; i < to.length; i++) {
      int idx = (int) index.getAndIncrement();
      if (idx >= N) {
        break;
      }
      count++;
      to[i] = idx;
    }
    return count;
  }

  private static String[] getSetArgs(int[] idxs, int len) {
    String[] ret = new String[2 * len];
    for (int i = 0; i < len; i++) {
      UserSession us = userSessions.get(idxs[i]);
      String skey = us.getUserId();
      String svalue = us.toString();
      ret[2 * i] = skey;
      ret[2 * i + 1] = svalue;
    }
    return ret;
  }

  private static String[] getGetArgs(int[] idxs, int len) {
    String[] ret = new String[len];
    for (int i = 0; i < len; i++) {
      UserSession us = userSessions.get(idxs[i]);
      String skey = us.getUserId();
      ret[i] = skey;
    }
    return ret;
  }

  private static void runLoad() throws IOException, OperationFailedException {

    Jedis client = getClient();
    totalDataSize = 0;

    long startTime = System.currentTimeMillis();
    int count = 0;
    int[] idxs = new int[20];
    int batches = 1;
    for (;;) {
      // int idx = (int) index.getAndIncrement();
      // if (idx >= N) break;
      // UserSession us = userSessions.get(idx);
      // count++;
      // String skey = us.getUserId();
      // String svalue = us.toString();
      // totalDataSize += skey.length() + svalue.length();
      // client.set(skey, svalue);
      int len = getIndexes(idxs);
      if (len == 0) {
        break;
      }
      String[] args = getSetArgs(idxs, len);
      client.mset(args);
      count += len;
      if (count / 100000 >= batches) {
        log.debug("{}: set {}", Thread.currentThread().getId(), count);
        batches++;
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("{}: Loaded {} user sessions, total size={} in {}ms", Thread.currentThread().getId(),
      count, totalDataSize, endTime - startTime);

    client.close();
  }

  private static void runGets() throws IOException, OperationFailedException {

    Jedis client = getClient();
    totalDataSize = 0;

    long startTime = System.currentTimeMillis();
    int count = 0;
    int batches = 1;
    int[] idxs = new int[20];
    for (;;) {
      // int idx = (int) index.getAndIncrement();
      // if (idx >= N) break;
      // UserSession us = userSessions.get(idx);
      // count++;
      // String skey = us.getUserId();
      // String svalue = us.toString();
      // totalDataSize += skey.length() + svalue.length();
      // String v = client.get(skey);
      // assertTrue(v != null && v.length() == svalue.length());

      int len = getIndexes(idxs);
      if (len == 0) {
        break;
      }
      String[] args = getGetArgs(idxs, len);
      List<String> result = client.mget(args);
      count += len;
      if (count / 100000 >= batches) {
        log.debug("{}: get {}", Thread.currentThread().getId(), count);
        batches++;
      }
      assert (args.length == result.size());
      verify(result, idxs);

      // if (count % 10000 == 0) {
      // log.debug("{}: get {}", Thread.currentThread().getId(), count);
      // }
    }
    long endTime = System.currentTimeMillis();

    log.debug("{}: Read {} user sessions in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);
    client.close();
  }

  private static void verify(List<String> result, int[] idxs) {
    int size = result.size();
    for (int i = 0; i < size; i++) {
      UserSession us = userSessions.get(idxs[i]);
      String expected = us.toString();
      String value = result.get(i);
      assert (value != null);
      assert (expected.equals(value));
    }
  }
}
