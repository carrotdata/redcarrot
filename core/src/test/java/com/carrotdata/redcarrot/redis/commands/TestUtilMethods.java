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
package com.carrotdata.redcarrot.redis.commands;

import static org.junit.Assert.assertEquals;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.redis.util.Utils;
import org.junit.Test;

public class TestUtilMethods {

  private static final Logger log = LogManager.getLogger(TestUtilMethods.class);

  private String[] inline =
      new String[] {
        "SET key value PXAT 1000 NX GET",
        "SET key value PXAT 1000 XX GET",
        "SET key10 value PXAT 1000 NX GET",
        "SET key10 value PXAT 1000 XX GET"
      };

  private String[] expected =
      new String[] {
        "*7\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\nPXAT\r\n$4\r\n1000\r\n$2\r\nNX\r\n$3\r\nGET\r\n",
        "*7\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\nPXAT\r\n$4\r\n1000\r\n$2\r\nXX\r\n$3\r\nGET\r\n",
        "*7\r\n$3\r\nSET\r\n$5\r\nkey10\r\n$5\r\nvalue\r\n$4\r\nPXAT\r\n$4\r\n1000\r\n$2\r\nNX\r\n$3\r\nGET\r\n",
        "*7\r\n$3\r\nSET\r\n$5\r\nkey10\r\n$5\r\nvalue\r\n$4\r\nPXAT\r\n$4\r\n1000\r\n$2\r\nXX\r\n$3\r\nGET\r\n"
      };

  @Test
  public void testInlineConversion() {
    for (int i = 0; i < inline.length; i++) {
      String conv = Utils.inlineToRedisRequest(inline[i]);
      log.debug(inline[i]);
      log.debug(expected[i]);
      log.debug(conv);
      assertEquals(expected[i], conv);
    }
  }
}
