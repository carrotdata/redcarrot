/**
 * Copyright (C) 2021-present Carrot, Inc.
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by MongoDB, Inc.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the Server
 * Side Public License for more details.
 * <p>
 * You should have received a copy of the Server Side Public License along with this program. If
 * not, see <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.carrotdata.redcarrot.redis.util.Utils;

public class RawClusterClient {
  private static Logger logger = LogManager.getLogger(RawClusterClient.class);

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
      logger.error(e);
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

  /**
   * Shutdown Carrot node
   * @param channel socket channel
   * @param save save
   * @throws IOException
   */
  private void shutdown(SocketChannel channel, boolean save) throws IOException {
    writeRequest(buf,
      save ? new String[] { "SHUTDOWN", "SAVE" } : new String[] { "SHUTDOWN", "NOSAVE" });
    buf.flip();
    while (buf.hasRemaining()) {
      channel.write(buf);
    }
    buf.clear();
    // while (buf.position() == 0) {
    // // Hack
    // channel.read(buf);
    // }
    // buf.flip();
    // byte[] bytes = new byte[buf.limit()];
    // buf.get(bytes);
    // String result = new String(bytes);
    // System.out.println("result="+ result);
  }

  /**
   * Shutdown all carrot nodes
   */
  public void shutdownAll() {
    for (SocketChannel sc : connList) {
      try {
        shutdown(sc, true);
      } catch (IOException e) {
        logger.error(e);
      }
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
