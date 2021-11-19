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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.lists.Lists;

/**
 * Simple network server for MVP (minimum viable product) Scalability and performance is not a goal
 * #1 yet
 */
public class RedisServer {

  private static final Logger log = LogManager.getLogger(RedisServer.class);

  /** Executor service (request handlers) */
  static RequestHandlers service;

  /** In memory data store */
  static BigSortedMap store;

  /** I/O selector for async operations */
  static Selector selector;

  /** Server started (for testing) */
  static boolean started = false;

  /**
   * Has server started yet?
   *
   * @return true, false
   */
  public static boolean hasStarted() {
    return started;
  }

  /** Start server in a separate thread */
  public static void start() {
    new Thread(
            () -> {
              try {
                RedisServer.main(new String[] {});
              } catch (IOException e) {
                // TODO Auto-generated catch block
                log.error("StackTrace: ", e);
              }
            })
        .start();

    while (!hasStarted()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        log.error("StackTrace: ", e);
      }
      log.debug("started={}", started);
    }
  }

  /**
   * Network server Main method (entry point)
   *
   * @param args command line argument list (path to a configuration file)
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    log.debug("Carrot-Redis server starting ...");
    String confFilePath = args.length > 0 ? args[0] : null;
    initStore(confFilePath);
    log.debug("Internal store started");

    startRequestHandlers();
    log.debug("Executor service started");

    // Selector: multiplexor of SelectableChannel objects
    final Selector selector = Selector.open(); // selector is open here
    log.debug("Selector started");

    // ServerSocketChannel: selectable channel for stream-oriented listening sockets
    ServerSocketChannel serverSocket = ServerSocketChannel.open();
    log.debug("Server socket opened");

    int port = RedisConf.getInstance().getServerPort();
    InetSocketAddress serverAddr = new InetSocketAddress("localhost", port);

    // Binds the channel's socket to a local address and configures the socket to listen for
    // connections
    serverSocket.bind(serverAddr);
    // Adjusts this channel's blocking mode.
    serverSocket.configureBlocking(false);
    int ops = serverSocket.validOps();
    serverSocket.register(selector, ops, null);
    log.debug("Carrot-Redis server started on port = {}", port);

    started = true;

    Consumer<SelectionKey> action =
        key -> {
          try {
            if (!key.isValid()) return;
            if (key.isValid() && key.isAcceptable()) {
              SocketChannel client = serverSocket.accept();
              // Adjusts this channel's blocking mode to false
              client.configureBlocking(false);
              client.setOption(StandardSocketOptions.TCP_NODELAY, true);
              // Operation-set bit for read operations
              client.register(selector, SelectionKey.OP_READ);
              log.debug("Connection Accepted: {}", client.getLocalAddress());
            } else if (key.isValid() && key.isReadable()) {
              // Check if it is in use
              RequestHandlers.Attachment att = (RequestHandlers.Attachment) key.attachment();
              if (att != null && att.inUse()) return;
              service.submit(key);
            }
          } catch (IOException e) {
            log.debug("Shutting down server ...");
            service.shutdown();
            store.dispose();
            store = null;
            service = null;
            log.debug("Bye-bye folks. See you soon :)");
          }
        };
    // Infinite loop..
    // Keep server running
    while (true) {
      // Selects a set of keys whose corresponding channels are ready for I/O operations
      selector.select(action);
    }
  }

  public static void shutdown() {
    if (selector != null) {
      try {
        selector.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        log.error("StackTrace: ", e);
      }
    }
  }

  private static void initStore(String confFilePath) {
    RedisConf conf = RedisConf.getInstance(confFilePath);
    long limit = conf.getMaxMemoryLimit();
    store = BigSortedMap.loadStore(0);
    if (store == null) {
      store = new BigSortedMap(limit);
    }
    // TODO: Load data from a configured snapshot directory
    BigSortedMap.setCompressionCodec(conf.getCompressionCodec());
    // Register custom memory deallocator for LIST data type
    Lists.registerDeallocator();
  }

  private static void startRequestHandlers() {
    RedisConf conf = RedisConf.getInstance();
    int numThreads = conf.getWorkingThreadPoolSize();
    service = RequestHandlers.create(store, numThreads);
    service.start();
  }
}
