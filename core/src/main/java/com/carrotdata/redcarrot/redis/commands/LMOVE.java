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

import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.lists.Lists;
import com.carrotdata.redcarrot.redis.lists.Lists.Side;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class LMOVE implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs != 5) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    // skip command name
    inDataPtr = skip(inDataPtr, 1);
    // read src key
    int srcKeySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long srcKeyPtr = inDataPtr;
    inDataPtr += srcKeySize;

    // read dst key
    int dstKeySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long dstKeyPtr = inDataPtr;
    inDataPtr += srcKeySize;

    // src LEFT | RIGHT
    Side srcSide = Side.LEFT;
    // dst LEFT | RIGHT
    Side dstSide = Side.LEFT;

    int flagSize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long flagPtr = inDataPtr;
    inDataPtr += flagSize;

    if (Utils.compareTo(LEFT_FLAG, LEFT_LENGTH, flagPtr, flagSize) == 0
        || Utils.compareTo(LEFT_FLAG_LOWER, LEFT_LENGTH, flagPtr, flagSize) == 0) {
      srcSide = Side.LEFT;
    } else if (Utils.compareTo(RIGHT_FLAG, RIGHT_LENGTH, flagPtr, flagSize) == 0
        || Utils.compareTo(RIGHT_FLAG_LOWER, RIGHT_LENGTH, flagPtr, flagSize) == 0) {
          srcSide = Side.RIGHT;
        } else {
          Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_COMMAND_FORMAT,
            ": " + Utils.toString(flagPtr, flagSize));
          return;
        }

    flagSize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    flagPtr = inDataPtr;
    inDataPtr += flagSize;

    if (Utils.compareTo(LEFT_FLAG, LEFT_LENGTH, flagPtr, flagSize) == 0
        || Utils.compareTo(LEFT_FLAG_LOWER, LEFT_LENGTH, flagPtr, flagSize) == 0) {
      dstSide = Side.LEFT;
    } else if (Utils.compareTo(RIGHT_FLAG, RIGHT_LENGTH, flagPtr, flagSize) == 0
        || Utils.compareTo(RIGHT_FLAG_LOWER, RIGHT_LENGTH, flagPtr, flagSize) == 0) {
          dstSide = Side.RIGHT;
        } else {
          Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_COMMAND_FORMAT,
            ": " + Utils.toString(flagPtr, flagSize));
          return;
        }

    int off = +Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
    int size = Lists.LMOVE(map, srcKeyPtr, srcKeySize, dstKeyPtr, dstKeySize, srcSide, dstSide,
      outBufferPtr + off, outBufferSize - off);

    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());
    UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size);
  }
}
