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
package com.carrotdata.redcarrot.redis.commands;

public class TestHMSET extends CommandBase {

  protected String[] validRequests =
      new String[] { "HMSET key1 field1 value1 field2 value2 field3 value3 field4 value4", /* 4 */
          "hmset key1 field1 value1 field2 value2 field3 value3 field4 value4", /* 4 */
          "HGET key1 field1", "HGET key1 field2", "HGET key1 field3", "HGET key1 field4" };

  protected String[] validResponses = new String[] { "+OK\r\n", "+OK\r\n", "$6\r\nvalue1\r\n",
      "$6\r\nvalue2\r\n", "$6\r\nvalue3\r\n", "$6\r\nvalue4\r\n" };

  protected String[] invalidRequests = new String[] { "HMSET", /* wrong number of arguments */
      "HMSET x", /* wrong number of arguments */
      "HMSET x y", /* wrong number of arguments */
      "HMSET x y z zz", /* wrong number of arguments */
      "HMSET x y z a b bb", /* wrong number of arguments */
  };

  protected String[] invalidResponses = new String[] { "-ERR: Wrong number of arguments\r\n",
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
