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

public class TestSETNX extends CommandBase {

  protected String[] validRequests =
      new String[] { "SETNX key value", /* 1 */ "setnx key value1" /* 0 */ };

  protected String[] validResponses = new String[] { ":1\r\n", ":0\r\n" };

  protected String[] invalidRequests = new String[] { "setnxx x y", /* unsupported command */
      "SETNX", /* wrong number of arguments */
      "SETNX key", /* wrong number of arguments */
      "SETNX key XXX value", /* wrong number of arguments */
  };

  protected String[] invalidResponses =
      new String[] { "-ERR: Unsupported command: SETNXX\r\n", "-ERR: Wrong number of arguments\r\n",
          "-ERR: Wrong number of arguments\r\n", "-ERR: Wrong number of arguments\r\n", };

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
