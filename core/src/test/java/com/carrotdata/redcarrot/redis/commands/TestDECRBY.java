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

public class TestDECRBY extends CommandBase {

  protected String[] validRequests =
      new String[] { "DECRBY key 100", "decrby key 100", "DECRBY key 100" };

  protected String[] validResponses = new String[] { ":-100\r\n", ":-200\r\n", ":-300\r\n", };

  protected String[] invalidRequests = new String[] { "DECRBY", /* wrong number of arguments */
      "DECRBY x", /* wrong number of arguments */
      "DECRBY x y z", /* wrong number of arguments */
      "DECRBY x y", /* wrong number format */
      "SET key1 value1", /* OK */
      "DECRBY key1 1" /* wrong number format */
  };

  protected String[] invalidResponses =
      new String[] { "-ERR: Wrong number of arguments\r\n", "-ERR: Wrong number of arguments\r\n",
          "-ERR: Wrong number of arguments\r\n", "-ERR: Wrong number format: y\r\n", "+OK\r\n",
          "-ERR: Wrong number format: Value at key is not a number\r\n" };

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
