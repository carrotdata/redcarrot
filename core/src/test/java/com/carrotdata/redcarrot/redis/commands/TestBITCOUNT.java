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

public class TestBITCOUNT extends CommandBase {

  protected String[] validRequests = new String[] { "SETBIT key 100 1", /* 0 */
      "SETBIT key 101 1", /* 0 */
      "SETBIT key 102 1", /* 0 */
      "SETBIT key 103 1", /* 0 */
      "SETBIT key 104 1", /* 0 */
      "bitcount key", /* 5 */
      "BITCOUNT key 0 11", /* 0 */
      "BITCOUNT key 0 12", /* 4 */
      "BITCOUNT key 0 13", /* 5 */
      "BITCOUNT key 0 14", /* 5 */
      "BITCOUNT key 0 1000", /* 5 */
      "BITCOUNT key 0 -1", /* 5 */
      "BITCOUNT key 105 110", /* 0 */
      "bitcount key1", /* 0 */
  };

  protected String[] validResponses =
      new String[] { ":0\r\n", ":0\r\n", ":0\r\n", ":0\r\n", ":0\r\n", ":5\r\n", ":0\r\n", ":4\r\n",
          ":5\r\n", ":5\r\n", ":5\r\n", ":5\r\n", ":0\r\n", ":0\r\n" };

  protected String[] invalidRequests = new String[] { "BITCOUNT", /* wrong number of arguments */
      "BITCOUNT key 1", /* wrong number of arguments */
      "BITCOUNT key 1 2 3", /* wrong number of arguments */
      "BITCOUNT key w 2", /* wrong number format */
      "BITCOUNT key 1 z", /* wrong number format */
  };

  protected String[] invalidResponses = new String[] { "-ERR: Wrong number of arguments\r\n",
      "-ERR: Wrong number of arguments\r\n", "-ERR: Wrong number of arguments\r\n",
      "-ERR: Wrong number format: w\r\n", "-ERR: Wrong number format: z\r\n" };

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
