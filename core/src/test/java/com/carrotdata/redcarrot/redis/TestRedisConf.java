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
package com.carrotdata.redcarrot.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.redis.util.Utils;
import org.junit.Before;
import org.junit.Test;

public class TestRedisConf {

  private static final Logger log = LogManager.getLogger(TestRedisConf.class);

  private RedisConf conf;
  String sconf = "#Carrot Redis server configuration\n" + "# Number of supported Redis commands\n"
      + "command.count=104\n" + "# Compression (NONE, LZ4)\n" + "compression.codec=none\n"
      + "# Data store maximum size\n" + "max.memory.limit=10000000000\n"
      + "# Maximum sorted size compact size\n" + "zset.compact.maxsize=512\n" + "# Server port\n"
      + "server.port=6379\n" + "# Thread pool size\n" + "thread.pool.size=1\n" + "# Cluster nodes\n"
      + "redis.nodes=127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382";

  @Before
  public void setUp() throws IOException {
    ByteArrayInputStream is = new ByteArrayInputStream(sconf.getBytes());
    Properties p = new Properties();
    p.load(is);
    conf = new RedisConf(p);
  }

  @Test
  public void testClusterSlots() {
    ByteBuffer buf = ByteBuffer.allocate(2048);
    Object[] slots = conf.getClusterSlots();
    Utils.serializeTypedArray(slots, buf);
    buf.flip();
    byte[] arr = new byte[buf.limit()];
    buf.get(arr);
    String result = new String(arr);
    log.debug(result);
  }

  @Test
  public void testDataBlockSizes() {
    Properties p = new Properties();
    p.setProperty(RedisConf.DATA_BLOCK_SIZES_KEY, "10,20,30");
    RedisConf conf = new RedisConf(p);

    int[] sizes = conf.getDataBlockSizes();
    assertEquals(3, sizes.length);
    assertEquals(10, sizes[0]);
    assertEquals(20, sizes[1]);
    assertEquals(30, sizes[2]);

    p.clear();
    p.setProperty(RedisConf.DATA_BLOCK_SIZES_KEY, "  10,20,50 ");
    sizes = conf.getDataBlockSizes();
    assertEquals(3, sizes.length);
    assertEquals(10, sizes[0]);
    assertEquals(20, sizes[1]);
    assertEquals(50, sizes[2]);

    p.clear();
    // Wrong format
    p.setProperty(RedisConf.DATA_BLOCK_SIZES_KEY, "10, xx,50");
    sizes = conf.getDataBlockSizes();
    assertNull(sizes);
  }
}
