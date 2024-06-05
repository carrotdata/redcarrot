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

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.zsets.ZSets;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class ZINCRBY implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs != 4) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      inDataPtr += Utils.SIZEOF_INT;
      // skip command name
      inDataPtr = skip(inDataPtr, 1);
      // read key
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;
      // read increment
      int incrSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      double incrValue = Utils.strToDouble(inDataPtr, incrSize);
      inDataPtr += incrSize;
      // read field
      int fieldSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long fieldPtr = inDataPtr;
      inDataPtr += fieldSize;

      double newValue = ZSets.ZINCRBY(map, keyPtr, keySize, incrValue, fieldPtr, fieldSize);
      DOUBLE_REPLY(outBufferPtr, outBufferSize, newValue);
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT,
        ": " + e.getMessage());
    }
  }
}
