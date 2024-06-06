/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.examples.geoip;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.sets.Sets;
import com.carrotdata.redcarrot.util.Bytes;
import com.carrotdata.redcarrot.util.UnsafeAccess;

import redis.clients.jedis.Jedis;

public class CityBlock {

  private static final Logger log = LogManager.getLogger(CityBlock.class);

  private String address;
  private long netAddress;
  private int id;
  private int maskBits;

  private CityBlock(int id, String ip) {
    this.id = id;
    this.netAddress = getNetworkAddress(ip);
  }

  @SuppressWarnings("deprecation")
  public static List<CityBlock> load(String file) throws IOException {
    File f = new File(file);
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    // skip CSV header
    dis.readLine();
    List<CityBlock> list = new ArrayList<CityBlock>();
    String line = null;
    int total = 0;
    while ((line = dis.readLine()) != null) {
      String[] parts = line.split(",");
      String s = parts[1];
      if (s.length() == 0) {
        s = parts[2];
      }
      int id = Integer.parseInt(s);
      CityBlock block = new CityBlock(id, parts[0]);
      list.add(block);
      total++;
      if (total % 100000 == 0) {
        log.debug("Loaded {}", total);
      }
    }
    dis.close();
    return list;
  }

  private long getNetworkAddress(String s) {
    String[] parts = s.split("/");
    this.address = parts[0];
    this.maskBits = Integer.valueOf(parts[1]);
    long ip = ip2long(parts[0]);
    int mask = (-1) << (32 - this.maskBits);
    return ip & mask;
  }

  private long ip2long(String s) {
    String[] parts = s.split("\\.");
    long v1 = Integer.parseInt(parts[0]);
    long v2 = Integer.parseInt(parts[1]);
    long v3 = Integer.parseInt(parts[2]);
    long v4 = Integer.parseInt(parts[3]);
    return v1 << 24 | v2 << 16 | v3 << 8 | v4;
  }

  public void saveToCarrot(BigSortedMap map, long keyPtr, int keySize) {
    long ptr = UnsafeAccess.malloc(12);
    int size = 12;
    UnsafeAccess.putLong(ptr, netAddress);
    UnsafeAccess.putInt(ptr + 8, id);
    Sets.SADD(map, keyPtr, keySize, ptr, size);
    UnsafeAccess.free(ptr);
  }

  public void saveToRedis(Jedis client, byte[] key) {
    client.zadd(key, netAddress, Bytes.add(Bytes.toBytes(netAddress), Bytes.toBytes(id)));
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(address).append(":").append(maskBits).append(":").append(id).append(":")
        .append(Long.toBinaryString(netAddress));
    return sb.toString();
  }

  public static void main(String[] args) throws IOException {
    List<CityBlock> list = load(args[0]);

    log.debug("Loaded {}", list.size());

    for (int i = 0; i < 20; i++) {
      log.debug("{}", list.get(i));
    }
  }
}
