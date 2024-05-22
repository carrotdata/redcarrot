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
import com.carrotdata.redcarrot.redis.zsets.ZSets;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class ZLEXCOUNT implements RedisCommand {

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

      long startPtr = 0, endPtr = 0;
      int startSize = 0, endSize = 0;
      boolean startInclusive = true, endInclusive = true;

      startSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      startPtr = inDataPtr;
      if (UnsafeAccess.toByte(startPtr) == (byte) '(') {
        // start is not inclusive
        startInclusive = false;
      } else if (UnsafeAccess.toByte(startPtr) == (byte) '[') {
        // start is inclusive
        startInclusive = true;
      } else if (UnsafeAccess.toByte(startPtr) == (byte) '-' && startSize == 1) {
        startPtr = 0;
        startSize = 0;
      } else {
        throw new IllegalArgumentException(Errors.ERR_MIN_SPECIFIED);
      }
      inDataPtr += Utils.SIZEOF_BYTE;
      startPtr = startPtr == 0 ? 0 : startPtr + Utils.SIZEOF_BYTE;
      startSize = startSize == 0 ? 0 : startSize - Utils.SIZEOF_BYTE;
      inDataPtr += startSize;

      endSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      endPtr = inDataPtr;

      if (UnsafeAccess.toByte(endPtr) == (byte) '(') {
        // start is not inclusive
        endInclusive = false;
      } else if (UnsafeAccess.toByte(endPtr) == (byte) '[') {
        // start is inclusive
        endInclusive = true;
      } else if (UnsafeAccess.toByte(endPtr) == (byte) '+' && endSize == 1) {
        endPtr = 0;
        endSize = 0;
      } else {
        throw new IllegalArgumentException(Errors.ERR_MAX_SPECIFIED);
      }
      endPtr = endPtr == 0 ? 0 : endPtr + Utils.SIZEOF_BYTE;
      endSize = endSize == 0 ? 0 : endSize - Utils.SIZEOF_BYTE;

      long num =
          ZSets.ZLEXCOUNT(
              map,
              keyPtr,
              keySize,
              startPtr,
              startSize,
              startInclusive,
              endPtr,
              endSize,
              endInclusive);
      // INTEGER reply - we do not check buffer size here - should be larger than 9
      INT_REPLY(outBufferPtr, num);

    } catch (IllegalArgumentException ee) {
      Errors.write(
          outBufferPtr,
          Errors.TYPE_GENERIC,
          Errors.ERR_WRONG_COMMAND_FORMAT,
          ": " + ee.getMessage());
    }
  }
}
