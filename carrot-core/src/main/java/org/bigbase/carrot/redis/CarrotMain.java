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
package org.bigbase.carrot.redis;

import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.lists.Lists;

/** Main service launcher */
public class CarrotMain {

  private static final Logger log = LogManager.getLogger(CarrotMain.class);

  public static void main(String[] args) {
    log.info("Start CarrotMain main...");
    if (args.length == 0) {
      usage();
    }
    loadConfigAndInit(args[0]);
    startNodes();
  }

  private static void startNodes() {
    log.trace("CarrotMain startNodes...");
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
      log.debug("Start nodeServers '{}'}", nodeServers[i]);
    }

    // Wait for all of them
    for (CarrotNodeServer nodeServer : nodeServers) {
      log.debug("Wait for shutdown nodeServers '{}'", nodeServer);
      nodeServer.join();
    }
    // shutdown
    log.info("[{}] Shutdown finished.", Thread.currentThread().getName());
  }

  private static void usage() {
    log.info("[{}] Usage: java org.bigbase.carrot.redis.RedisMain config_file_path",Thread.currentThread().getName());
    System.exit(-1);
  }

  private static void loadConfigAndInit(String confFilePath) {
    log.trace("CarrotMain loadConfigAndInit...");
    RedisConf conf = RedisConf.getInstance(confFilePath);
    long limit = conf.getMaxMemoryLimit();
    log.debug("Max limit memory '{}'", limit);
    BigSortedMap.setGlobalMemoryLimit(limit);
    BigSortedMap.setCompressionCodec(conf.getCompressionCodec());
    log.debug("setCompressionCodec '{}'", conf.getCompressionCodec());
    // Register custom memory deallocator for LIST data type
    Lists.registerDeallocator();
    Lists.registerSerDe();
  }
}
