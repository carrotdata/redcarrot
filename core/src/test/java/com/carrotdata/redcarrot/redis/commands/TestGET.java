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

public class TestGET extends CommandBase {

  protected String[] validRequests = new String[] { "SET key1 100", "GET key1", "get key2" };

  protected String[] validResponses = new String[] { "+OK\r\n", "$3\r\n100\r\n", "$-1\r\n" };

  protected String[] invalidRequests = new String[] { "GET", /* wrong arg number */ "get", /*
                                                                                            * wrong
                                                                                            * arg
                                                                                            * number
                                                                                            */
      "get x y", /* wrong arg number */ "GET x y", /* wrong arg number */
  };

  protected String[] invalidResponses =
      new String[] { "-ERR: Wrong number of arguments\r\n", "-ERR: Wrong number of arguments\r\n",
          "-ERR: Wrong number of arguments\r\n", "-ERR: Wrong number of arguments\r\n" };

  /** Subclasses must override */
  protected String[] getValidRequests() {
    return validRequests;
  }

  @Override
  protected String[] getValidResponses() {
    return validResponses;
  }

  @Override
  protected String[] getInvalidRequests() {
    return invalidRequests;
  }

  @Override
  protected String[] getInvalidResponses() {
    return invalidResponses;
  }
}
