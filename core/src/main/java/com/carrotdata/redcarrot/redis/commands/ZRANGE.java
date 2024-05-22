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

public class ZRANGE implements RedisCommand {

  /** TODO: v 6.2 support ZRANGE key min max [WITHSCORES] */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    boolean withScores = false;
    long min = 0, max = 0;
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs < 4 || numArgs > 5) {
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

      int valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long valPtr = inDataPtr;
      min = Utils.strToLong(valPtr, valSize);
      inDataPtr += valSize;

      valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      valPtr = inDataPtr;
      max = Utils.strToLong(valPtr, valSize);
      inDataPtr += valSize;

      if (numArgs == 5) {
        valSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        valPtr = inDataPtr;
        if (Utils.compareTo(WITHSCORES_FLAG, WITHSCORES_LENGTH, valPtr, valSize) == 0
            || Utils.compareTo(WITHSCORES_FLAG_LOWER, WITHSCORES_LENGTH, valPtr, valSize) == 0) {
          withScores = true;
        } else {
          throw new IllegalArgumentException(Utils.toString(valPtr, valSize));
        }
      }
      int off = Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
      int size =
          (int)
              ZSets.ZRANGE(
                  map,
                  keyPtr,
                  keySize,
                  min,
                  max,
                  withScores,
                  outBufferPtr + off,
                  outBufferSize - off);

      if (withScores) {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ZARRAY.ordinal());
      } else {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.VARRAY.ordinal());
      }

      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size + off);

    } catch (NumberFormatException e) {
      Errors.write(
          outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT, ": " + e.getMessage());
    } catch (IllegalArgumentException ee) {
      Errors.write(
          outBufferPtr,
          Errors.TYPE_GENERIC,
          Errors.ERR_WRONG_COMMAND_FORMAT,
          ": " + ee.getMessage());
    }
  }
}
