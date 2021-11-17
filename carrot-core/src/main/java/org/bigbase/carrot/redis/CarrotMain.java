/**
 * Copyright (C) 2021-present Carrot, Inc.
 *
 * <p>This program is free software: you can redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by MongoDB, Inc.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * <p>You should have received a copy of the Server Side Public License along with this program. If
 * not, see <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package org.bigbase.carrot.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.lists.Lists;

/** Main service launcher */
public class CarrotMain {

  private static Logger logger = LogManager.getLogger(CarrotMain.class);
  
  public static void main(String[] args) {
    if (args.length != 2) {
      usage();
    }
    if (args[1].equals("stop")) {
      stopServer(args[0]);
    } else if (args[1].equals("start")) {
      startServer(args[0]);
    } else {
      usage();
    }
  }

  private static void stopServer(String configFile) {
    System.out.println("Stopping Carrot Server");
    log("Stopping Carrot server ...");
    loadConfig(configFile);
    RedisConf conf = RedisConf.getInstance();
    String[] nodes = conf.getNodes();
    List<String> list = new ArrayList<String>();
    Arrays.stream(nodes).forEach(n -> list.add(n));
    RawClusterClient client = new RawClusterClient(list);
    client.shutdownAll();
    // shutdown
    log("Shutdown finished.");
  }


  private static void startServer(String configFile) {
    
    log("Starting Carrot server ...");

    loadConfigAndInit(configFile);
    RedisConf conf = RedisConf.getInstance();
    String[] nodes = conf.getNodes();
    CarrotNodeServer[] nodeServers = new CarrotNodeServer[nodes.length];
    CarrotNodeServer.readyToStartLatch = new CountDownLatch(nodes.length);
    // Disable global statistics update during snapshot data loading
    BigSortedMap.setStatsUpdatesDisabled(true);
    for (int i = 0; i < nodes.length; i++) {
      String[] parts = nodes[i].split(":");
      String host = parts[0].trim();
      int port = Integer.parseInt(parts[1].trim());
      nodeServers[i] = new CarrotNodeServer(host, port);
      nodeServers[i].start();
    }

    // Wait for all of them
    for (int i = 0; i < nodeServers.length; i++) {
      nodeServers[i].join();
    }
    // shutdown
    log("Shutdown finished.");
  }

  private static void usage() {
    log("Usage: java org.bigbase.carrot.redis.CarrotMain config_file_path [start|stop]");
    System.exit(-1);
  }

  private static void loadConfigAndInit(String confFilePath) {
    RedisConf conf = RedisConf.getInstance(confFilePath);
    long limit = conf.getMaxMemoryLimit();
    BigSortedMap.setGlobalMemoryLimit(limit);
    BigSortedMap.setCompressionCodec(conf.getCompressionCodec());
    // Register custom memory deallocator for LIST data type
    Lists.registerDeallocator();
    Lists.registerSerDe();
  }

  private static void loadConfig(String confFilePath) {
    RedisConf conf = RedisConf.getInstance(confFilePath);
  }
  
  static void log(String str) {
    logger.info("[{}] {}", Thread.currentThread().getName(), str);
  }

  static void logError(String str) {
    logger.error("[{}] {}", Thread.currentThread().getName(), str);
  }
}
