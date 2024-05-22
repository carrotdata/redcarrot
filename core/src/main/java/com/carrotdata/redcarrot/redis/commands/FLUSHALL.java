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

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.server.Server;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class FLUSHALL implements RedisCommand {
  // TODO: ASYNC support
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs > 2) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }

    inDataPtr += Utils.SIZEOF_INT;
    inDataPtr = skip(inDataPtr, 1);

    // We ignore schedule flag for now
    if (numArgs == 2) {
      int size = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;

      if (Utils.compareTo(SYNC_FLAG, SYNC_LENGTH, inDataPtr, size) != 0
          && Utils.compareTo(SYNC_FLAG_LOWER, SYNC_LENGTH, inDataPtr, size) != 0
          && Utils.compareTo(ASYNC_FLAG, ASYNC_LENGTH, inDataPtr, size) != 0
          && Utils.compareTo(ASYNC_FLAG_LOWER, ASYNC_LENGTH, inDataPtr, size) != 0) {
        Errors.write(
            outBufferPtr,
            Errors.TYPE_GENERIC,
            Errors.ERR_UNSUPPORTED_COMMAND,
            ": FLUSHALL " + Utils.toString(inDataPtr, size));
        return;
      }
    }
    Server.FLUSHALL(map);
  }
}
