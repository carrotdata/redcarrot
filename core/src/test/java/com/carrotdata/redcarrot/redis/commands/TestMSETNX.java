/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis.commands;

public class TestMSETNX extends CommandBase {

  protected String[] validRequests = new String[] { "SET key1 value1", /* OK */
      "GET key1", /* value1 */
      "MSETNX key1 value11 key2 value2 key3 value3 key4 value4", /* 0 */
      "msetnx key2 value2 key3 value3 key4 value4", /* 1 */
      "GET key1", /* still value1 */
      "GET key2", /* value2 */
      "GET key3", /* value3 */
      "GET key4", /* value4 */
  };

  protected String[] validResponses = new String[] { "+OK\r\n", "$6\r\nvalue1\r\n", ":0\r\n",
      ":1\r\n", "$6\r\nvalue1\r\n", "$6\r\nvalue2\r\n", "$6\r\nvalue3\r\n", "$6\r\nvalue4\r\n" };

  protected String[] invalidRequests = new String[] { "msetn x", /* unsupported command */
      "MSETNX", /* wrong number of arguments */
      "MSETNX x", /* wrong number of arguments */
      "MSETNX x y z", /* wrong number of arguments */
      "MSETNX x y z a b", /* wrong number of arguments */
  };

  protected String[] invalidResponses = new String[] { "-ERR: Unsupported command: MSETN\r\n",
      "-ERR: Wrong number of arguments\r\n", "-ERR: Wrong number of arguments\r\n",
      "-ERR: Wrong number of arguments\r\n", "-ERR: Wrong number of arguments\r\n" };

  /** Subclasses must override */
  protected String[] getValidRequests() {
    return validRequests;
  }

  protected String[] getValidResponses() {
    return validResponses;
  }

  protected String[] getInvalidRequests() {
    return invalidRequests;
  }

  protected String[] getInvalidResponses() {
    return invalidResponses;
  }
}
