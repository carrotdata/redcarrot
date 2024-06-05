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

public class ZCOUNT implements RedisCommand {

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

      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;

      // FIXME - double conversion
      double start = 0, end = 0;
      boolean startInclusive = true, endInclusive = true;

      int valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long valPtr = inDataPtr;
      if (UnsafeAccess.toByte(valPtr) == (byte) '(') {
        valPtr += Utils.SIZEOF_BYTE;
        valSize -= Utils.SIZEOF_BYTE;
        // start is not inclusive
        startInclusive = false;
        inDataPtr += Utils.SIZEOF_BYTE;
      }
      start = Utils.strToDouble(valPtr, valSize);

      inDataPtr += valSize;
      valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      valPtr = inDataPtr;
      if (UnsafeAccess.toByte(valPtr) == (byte) '(') {
        valPtr += Utils.SIZEOF_BYTE;
        valSize -= Utils.SIZEOF_BYTE;
        // start is not inclusive
        endInclusive = false;
        inDataPtr += Utils.SIZEOF_BYTE;
      }
      end = Utils.strToDouble(valPtr, valSize);

      long num = ZSets.ZCOUNT(map, keyPtr, keySize, start, startInclusive, end, endInclusive);

      // INTEGER reply - we do not check buffer size here - should n=be larger than 9
      INT_REPLY(outBufferPtr, num);

    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT,
        ": " + e.getMessage());
    }
  }
}
