/**
 * Copyright (C) 2021-present Carrot, Inc.
 *
 * <p>This program is free software: you can redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by MongoDB, Inc.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * <p>You should have received a copy of the Server Side Public License along with this program. If
 * not, see <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis.commands;

public class TestLASTSAVE extends CommandBase {

  protected String[] validRequests = new String[] {"SAVE" /* OK  */, "LASTSAVE", "lastsave"};

  protected String[] validResponses = new String[] {"+OK\r\n", SKIP_VERIFY, SKIP_VERIFY};

  protected String[] invalidRequests =
      new String[] {
        "lastsav x y", /* unsupported command */ "LASTSAVE COUNT X", /* wrong number of arguments*/
      };

  protected String[] invalidResponses =
      new String[] {
        "-ERR: Unsupported command: LASTSAV\r\n", "-ERR: Wrong number of arguments\r\n",
      };

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
