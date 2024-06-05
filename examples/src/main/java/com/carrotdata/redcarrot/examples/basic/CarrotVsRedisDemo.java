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
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.examples.util.UserSession;
import com.carrotdata.redcarrot.ops.OperationFailedException;
import com.carrotdata.redcarrot.redis.util.Utils;

@SuppressWarnings("unused")
public class CarrotVsRedisDemo {

  private static final Logger log = LogManager.getLogger(CarrotVsRedisDemo.class);

  static long N = 10000000;

  static AtomicLong index = new AtomicLong(0);

  static int NUM_THREADS = 2;
  static int BATCH_SIZE = 100;
  static int SET_SIZE = 1000;
  static int SET_KEY_TOTAL = 100000;
  static AtomicLong loaded = new AtomicLong(0);
  static long totalLoaded = 0;

  static List<String> clusterNodes = Arrays.asList(
    "localhost:6379" /*
                      * , "localhost:6380", "localhost:6381", "localhost:6382", "localhost:6383",
                      * "localhost:6384", "localhost:6385", "localhost:6386"
                      */);

  public static void main(String[] args) throws IOException, OperationFailedException {

    log.debug("Run Carrot vs. Redis Demo");
    RawClusterClient client = new RawClusterClient(clusterNodes);
    long start = System.currentTimeMillis();
    flushAll(client);
    long end = System.currentTimeMillis();
    log.debug("flush all {}ms", end - start);

    // main 3 tests
    start = System.currentTimeMillis();

    // Load sets with cardinality 1000 each
    runClusterSaddCycle();
    totalLoaded += loaded.get();
    loaded.set(0);

    flushAll(client);

    // Load hashes, each 1000 length
    runClusterHSetCycle();
    totalLoaded += loaded.get();
    loaded.set(0);

    flushAll(client);

    // Load ordered sets each 1000 elements length
    runClusterZAddCycle();
    totalLoaded += loaded.get();

    shutdownAll(client, true);

    end = System.currentTimeMillis();

    NumberFormat formatter = NumberFormat.getInstance();

    log.debug("\n************************ RESULTS *************************************");
    log.debug("\nTOTAL ELEMENTS LOADED ={} TOTAL TIME ={}s, EPS ={}", formatter.format(totalLoaded),
      (double) (end - start) / 1000, formatter.format(totalLoaded * 1000 / (end - start)));

    client.close();
  }

  private static void runClusterSetCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterSet();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} sets in {}ms.RPS={}", N, end - start, N * 1000 / (end - start));
  }

  private static void runClusterGetCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterGet();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} sets in {}ms.RPS={}", N, end - start, N * 1000 / (end - start));
  }

  private static void runClusterMGetCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterMGet();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} sets in {}ms.RPS={}", N, end - start, N * 1000 / (end - start));
  }

  private static void runClusterMSetCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterMSet();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} sets in {}ms.RPS={}", N, end - start, N * 1000 / (end - start));
  }

  private static void runClusterPingPongCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterPingPong();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} ping - pongs in {}ms. RPS={}", N * NUM_THREADS, end - start,
      NUM_THREADS * N * 1000 / (end - start));
  }

  private static void runClusterSaddCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterSadd();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} sadd x ({}) in {}ms RPS={}", loaded.get() / SET_SIZE, SET_SIZE,
      end - start, loaded.get() * 1000 / (end - start));
  }

  private static void runClusterSismemberCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterSetIsMember();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} sismember x ({}) in {}ms. RPS={}", SET_KEY_TOTAL, SET_SIZE, end - start,
      (long) SET_KEY_TOTAL * SET_SIZE * 1000 / (end - start));
  }

  private static void runClusterHSetCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterHSet();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} hset x ({}) in {}ms/ RPS={}", loaded.get() / SET_SIZE, SET_SIZE,
      end - start, loaded.get() * 1000 / (end - start));
  }

  private static void runClusterHexistsCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterHexists();
        ;
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} hexists x ({}) in {}ms/ RPS={}", SET_KEY_TOTAL, SET_SIZE, end - start,
      (long) SET_KEY_TOTAL * SET_SIZE * 1000 / (end - start));
  }

  private static void runClusterZAddCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterZAdd();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} zadd x ({}) in {}ms. RPS={}", loaded.get() / SET_SIZE, SET_SIZE,
      end - start, loaded.get() * 1000 / (end - start));
  }

  private static void runClusterZScoreCycle() {
    index.set(0);

    Runnable r = () -> {
      try {
        runClusterZScore();
      } catch (Exception e) {
        log.error("StackTrace: ", e);
      }
    };
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
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

    log.debug("Finished {} hexists x ({}) in {}ms. RPS={}", SET_KEY_TOTAL, SET_SIZE, end - start,
      (long) SET_KEY_TOTAL * SET_SIZE * 1000 / (end - start));
  }

  private static void flushAll(RawClusterClient client) throws IOException {
    client.flushAll();
  }

  private static void shutdownAll(RawClusterClient client, boolean save) throws IOException {
    long start = System.currentTimeMillis();
    client.saveAll();
    long end = System.currentTimeMillis();
    log.debug("Save time={}", end - start);
  }

  private static void runClusterSet() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} SET started. , connect to :{}", Thread.currentThread().getName(), list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;

    for (;;) {
      int idx = (int) index.getAndIncrement();
      if (idx >= N) break;
      UserSession us = UserSession.newSession(idx); // userSessions.get(idx);
      count++;
      String skey = us.getUserId();
      String svalue = us.toString();
      String v = client.set(skey, svalue);
      // assertTrue(v.indexOf(svalue) > 0);
      if (count % 100000 == 0) {
        log.debug("{}: set {}", Thread.currentThread().getId(), count);
      }
    }

    long endTime = System.currentTimeMillis();

    log.debug("{}: Loaded {} user sessions, in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);

    client.close();
  }

  private static void runClusterMSet() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} SET started. , connect to :{}", Thread.currentThread().getName(), list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    List<String> argList = new ArrayList<String>();
    int batch = 1;
    while (idx < N) {

      // Load up to BATCH_SIZE k-v pairs
      for (int i = 0; i < BATCH_SIZE; i++, idx += NUM_THREADS) {
        if (idx >= N) break;
        UserSession us = UserSession.newSession(idx); // userSessions.get(idx);
        count++;
        String skey = us.getUserId();
        String svalue = us.toString();
        argList.add(skey);
        argList.add(svalue);
      }

      if (argList.size() == 0) break;

      String[] args = new String[argList.size()];
      argList.toArray(args);
      client.mset(args);
      if (count / 100000 >= batch) {
        log.debug("{}: set {}", Thread.currentThread().getId(), count);
        batch++;
      }
      argList.clear();
    }

    long endTime = System.currentTimeMillis();

    log.debug("{}: Loaded {} user sessions, in {}", Thread.currentThread().getId(), count,
      endTime - startTime);

    client.close();
  }

  private static void runClusterSadd() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} SADD started. , connect to :{}", Thread.currentThread().getName(), list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    int batch = 1;
    String[] setMembers = new String[SET_SIZE];
    populate(setMembers, id);

    for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
      UserSession us = UserSession.newSession(idx);
      String key = us.getUserId();
      String result = client.sadd(key, setMembers);
      int added = toInt(result);
      count += added;
      if (added == 0) {
        break;
      }
      if (count / 1000000 >= batch) {
        log.debug("{}: sadd {}", Thread.currentThread().getId(), count);
        batch++;
      }
    }

    long endTime = System.currentTimeMillis();

    log.debug("{}: Loaded {} set members in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);
    loaded.addAndGet(count);
    client.close();
  }

  private static void runClusterSetIsMember() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} SISMBER started. , connect to :{}", Thread.currentThread().getName(),
      list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    int batch = 1;
    String[] setMembers = new String[SET_SIZE];
    populate(setMembers, id);

    Arrays.sort(setMembers);

    for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
      UserSession us = UserSession.newSession(idx);
      String key = us.getUserId();
      for (int i = 0; i < SET_SIZE; i++) {
        String member = setMembers[i];
        String result = client.sismember(key, member);
        if (!":1\r\n".equals(result)) {
          log.fatal("sismember failed result={}", result);
          System.exit(-1);
        }
      }
      count += SET_SIZE;
      if (count / 1000000 >= batch) {
        log.debug("{}: sismember {}", Thread.currentThread().getId(), count);
        batch++;
      }
    }

    long endTime = System.currentTimeMillis();

    log.debug("{}: Loaded {}  set members, in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);

    client.close();
  }

  private static void runClusterHSet() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} HSET started. , connect to :{}", Thread.currentThread().getName(), list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    int batch = 1;
    String[] setMembers = new String[SET_SIZE];
    populate(setMembers, id);
    setMembers = interleave(setMembers);
    for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
      UserSession us = UserSession.newSession(idx);
      String key = us.getUserId();
      String result = client.hset(key, setMembers);
      int added = toInt(result);
      count += added;
      if (added == 0) {
        break;
      }
      if (count / 1000000 >= batch) {
        log.debug("{}: hset {}", Thread.currentThread().getId(), count);
        batch++;
      }
    }

    long endTime = System.currentTimeMillis();
    loaded.addAndGet(count);
    log.debug("{}: Loaded {} hash members, in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);

    client.close();
  }

  private static String[] interleave(String[] arr) {
    String[] ret = new String[2 * arr.length];
    for (int i = 0, j = 0; i < arr.length; i++, j += 2) {
      ret[j] = arr[i];
      ret[j + 1] = arr[i];
    }
    return ret;
  }

  private static void runClusterHexists() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} HEXISTS started. , connect to :{}", Thread.currentThread().getName(),
      list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    int batch = 1;
    String[] setMembers = new String[SET_SIZE];
    populate(setMembers, id);

    Arrays.sort(setMembers);

    for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
      UserSession us = UserSession.newSession(idx);
      String key = us.getUserId();
      for (int i = 0; i < SET_SIZE; i++) {
        String member = setMembers[i];
        String result = client.hexists(key, member);
        if (!":1\r\n".equals(result)) {
          log.fatal("hexists failed result={}", result);
          System.exit(-1);
        }
      }
      count += SET_SIZE;
      if (count / 1000000 >= batch) {
        log.debug("{}: hexists {}", Thread.currentThread().getId(), count);
        batch++;
      }
    }

    long endTime = System.currentTimeMillis();
    log.debug("{}: Checked {} hash fields, in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);

    client.close();
  }

  private static void populate(String[] setMembers, long seed) {
    Random r = new Random(seed);
    for (int i = 0; i < setMembers.length; i++) {
      setMembers[i] = Integer.toString(r.nextInt());
    }
  }

  private static void runClusterZAdd() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} ZADD started. connect to :{}", Thread.currentThread().getName(), list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    int batch = 1;
    String[] setMembers = new String[SET_SIZE];
    populate(setMembers, id);
    double[] scores = new double[SET_SIZE];
    populate(scores, id);

    for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
      UserSession us = UserSession.newSession(idx);
      String key = us.getUserId();
      String result = client.zadd(key, scores, setMembers);
      int added = toInt(result);
      count += added;
      if (added == 0) {
        break;
      }
      if (count / 1000000 >= batch) {
        log.debug("{} : zadd {}", Thread.currentThread().getId(), count);
        batch++;
      }
    }

    long endTime = System.currentTimeMillis();
    loaded.addAndGet(count);
    log.debug("{}: Loaded {} zset members, in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);

    client.close();
  }

  private static int toInt(String s) {
    if (s.charAt(0) != ':') return 0;
    int len = s.length();
    return Integer.parseInt(s.substring(1, len - 2));
  }

  private static void populate(double[] scores, int id) {
    Random r = new Random(id);
    for (int i = 0; i < scores.length; i++) {
      scores[i] = r.nextDouble() * 1000;
    }
  }

  private static void runClusterZScore() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} ZSCORE started. , connect to :{}", Thread.currentThread().getName(), list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    int batch = 1;
    String[] setMembers = new String[SET_SIZE];
    populate(setMembers, id);
    double[] scores = new double[SET_SIZE];
    populate(scores, id);

    for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
      UserSession us = UserSession.newSession(idx);
      String key = us.getUserId();
      for (int i = 0; i < SET_SIZE; i++) {
        String member = setMembers[i];
        String result = client.zscore(key, member);
        if ("$-1\r\n".equals(result)) {
          log.fatal("zscore failed result={}", result);
          System.exit(-1);
        }
        String exp = Double.toString(scores[i]);
        if (!result.contains(exp)) {
          log.fatal("zscore failed result={}", result);
          System.exit(-1);
        }
      }
      count += SET_SIZE;
      if (count / 1000000 >= batch) {
        log.debug("{}: zscore {}} ", Thread.currentThread().getId(), count);
        batch++;
      }
    }

    long endTime = System.currentTimeMillis();

    log.debug("{}: Checked {}  zset fields, in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);

    client.close();
  }

  private static void runClusterGet() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} GET started. , connect to :{}", Thread.currentThread().getName(), list.get(0));
    long startTime = System.currentTimeMillis();
    int count = 0;
    for (;;) {
      int idx = (int) index.getAndIncrement();
      if (idx >= N) break;
      UserSession us = UserSession.newSession(idx); // userSessions.get(idx);
      count++;
      String skey = us.getUserId();
      String svalue = us.toString();
      String v = client.get(skey);
      // assertTrue(v.indexOf(svalue) > 0);
      if (count % 100000 == 0) {
        log.debug("{}: get {}", Thread.currentThread().getId(), count);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("{}: Read {} user sessions in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);
    client.close();
  }

  private static void runClusterMGet() throws IOException, OperationFailedException {

    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} GET started. , connect to :{}", Thread.currentThread().getName(), list.get(0));
    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    List<String> argList = new ArrayList<String>();
    // List<String> valList = new ArrayList<String>();
    int batch = 1;
    while (idx < N) {

      // Load up to BATCH_SIZE k-v pairs
      for (int i = 0; i < BATCH_SIZE; i++, idx += NUM_THREADS) {
        if (idx >= N) break;
        UserSession us = UserSession.newSession(idx); // .get(idx);
        count++;
        String skey = us.getUserId();
        // String svalue = us.toString();
        argList.add(skey);
        // valList.add(svalue);
      }

      if (argList.size() == 0) break;

      String[] args = new String[argList.size()];
      argList.toArray(args);
      String s = client.mget(args);
      if (s.length() < 1000) {
        log.debug("\n{}\n", s);
      }
      // verify(valList, s);
      if (count / 100000 >= batch) {
        log.debug("{}: get {} s.length={}", Thread.currentThread().getId(), count, s.length());
        batch++;
      }
      argList.clear();
      // valList.clear();
    }
    long endTime = System.currentTimeMillis();

    log.debug("{}: REad {} user sessions in {}ms", Thread.currentThread().getId(), count,
      endTime - startTime);
    client.close();
  }

  private static void verify(List<String> valList, String s) {
    for (String val : valList) {
      assert (s.indexOf(val) > 0);
    }
  }

  public static void runClusterPingPong() throws IOException {
    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    log.debug("{} PING/PONG started. , connect to :{}", Thread.currentThread().getName(),
      list.get(0));
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < N; i++) {
      String reply = client.ping();
      assert (reply.indexOf("PONG") > 0);
      if ((i + 1) % 100000 == 0) {
        log.debug("{}: pings {}", Thread.currentThread().getName(), i + 1);
      }
    }
    long endTime = System.currentTimeMillis();

    log.debug("{}: Ping-Pong {} messages in {}ms", Thread.currentThread().getId(), N,
      endTime - startTime);
    client.close();
  }

  static class RawClusterClient {

    byte[] CRLF = new byte[] { (byte) '\r', (byte) '\n' };
    byte ARRAY = (byte) '*';
    byte STR = (byte) '$';

    List<SocketChannel> connList;

    ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);

    public RawClusterClient(List<String> nodes) {
      try {
        connList = new ArrayList<SocketChannel>();
        for (String node : nodes) {
          connList.add(openConnection(node));
        }
      } catch (IOException e) {
        log.error("StackTrace: ", e);
      }
    }

    private SocketChannel openConnection(String node) throws IOException {
      String[] parts = node.split(":");
      String host = parts[0];
      int port = Integer.parseInt(parts[1]);

      SocketChannel sc = SocketChannel.open(new InetSocketAddress(host, port));
      sc.configureBlocking(false);
      sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
      sc.setOption(StandardSocketOptions.SO_SNDBUF, 64 * 1024);
      sc.setOption(StandardSocketOptions.SO_RCVBUF, 64 * 1024);
      return sc;
    }

    public String mset(String[] args) throws IOException {
      String[] newArgs = new String[args.length + 1];
      System.arraycopy(args, 0, newArgs, 1, args.length);
      newArgs[0] = "MSET";
      writeRequest(buf, newArgs);
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      buf.flip();
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String set(String key, String value) throws IOException {
      writeRequest(buf, new String[] { "SET", key, value });
      int slot = Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      buf.flip();
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String get(String key) throws IOException {
      writeRequest(buf, new String[] { "GET", key });
      int slot = Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      buf.flip();
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String mget(String[] keys) throws IOException {

      String[] newArgs = new String[keys.length + 1];
      System.arraycopy(keys, 0, newArgs, 1, keys.length);
      newArgs[0] = "MGET";
      writeRequest(buf, newArgs);
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      buf.flip();
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      int pos = buf.position();
      while (!Utils.arrayResponseIsComplete(buf)) {
        // Hack
        buf.position(pos);
        buf.limit(buf.capacity());
        channel.read(buf);
        pos = buf.position();
        continue;
      }
      buf.position(pos);
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public void close() throws IOException {
      for (SocketChannel sc : connList) {
        sc.close();
      }
    }

    static String[] ping_cmd = new String[] { "PING" };

    public String ping() throws IOException {
      int slot = 0;
      SocketChannel channel = connList.get(slot);
      writeRequest(buf, ping_cmd);
      buf.flip();
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    private String flushAll(SocketChannel channel) throws IOException {
      writeRequest(buf, new String[] { "FLUSHALL" });
      buf.flip();
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    private String save(SocketChannel channel) throws IOException {
      writeRequest(buf, new String[] { "SAVE" });
      buf.flip();
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public void flushAll() throws IOException {
      for (SocketChannel sc : connList) {
        flushAll(sc);
      }
    }

    public String sscan(String key, long cursor) throws IOException {
      writeRequest(buf, new String[] { "SSCAN", key, Long.toString(cursor) });
      buf.flip();
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String sadd(String key, String[] args) throws IOException {
      String[] newArgs = new String[args.length + 2];
      System.arraycopy(args, 0, newArgs, 2, args.length);
      newArgs[0] = "SADD";
      newArgs[1] = key;
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String sismember(String key, String v) throws IOException {
      String[] newArgs = new String[3];
      newArgs[0] = "SISMEMBER";
      newArgs[1] = key;
      newArgs[2] = v;

      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String hset(String key, String[] args) throws IOException {
      String[] newArgs = new String[args.length + 2];
      System.arraycopy(args, 0, newArgs, 2, args.length);
      newArgs[0] = "HSET";
      newArgs[1] = key;
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String hexists(String key, String field) throws IOException {
      String[] newArgs = new String[3];
      newArgs[0] = "HEXISTS";
      newArgs[1] = key;
      newArgs[2] = field;
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String expire(String key, int seconds) throws IOException {
      String[] newArgs = new String[3];
      newArgs[0] = "HEXISTS";
      newArgs[1] = key;
      newArgs[2] = Integer.toString(seconds);
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String zadd(String key, double[] scores, String[] fields) throws IOException {
      String[] newArgs = new String[2 * fields.length + 2];
      newArgs[0] = "ZADD";
      newArgs[1] = key;

      for (int i = 2; i < newArgs.length; i += 2) {
        newArgs[i] = Double.toString(scores[(i - 2) / 2]);
        newArgs[i + 1] = fields[(i - 2) / 2];
      }

      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public String zscore(String key, String field) throws IOException {
      String[] newArgs = new String[3];
      newArgs[0] = "ZSCORE";
      newArgs[1] = key;
      newArgs[2] = field;
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0; // Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();

      while (buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String(bytes);
    }

    public void saveAll() throws IOException {
      for (SocketChannel sc : connList) {
        save(sc);
      }
    }

    private void writeRequest(ByteBuffer buf, String[] args) {
      buf.clear();
      // Array
      buf.put(ARRAY);
      // number of elements
      buf.put(Integer.toString(args.length).getBytes());
      // CRLF
      buf.put(CRLF);
      for (int i = 0; i < args.length; i++) {
        buf.put(STR);
        buf.put(Integer.toString(args[i].length()).getBytes());
        buf.put(CRLF);
        buf.put(args[i].getBytes());
        buf.put(CRLF);
      }
    }
  }
}
