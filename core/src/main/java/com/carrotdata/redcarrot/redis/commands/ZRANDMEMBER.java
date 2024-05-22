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

public class ZRANDMEMBER implements RedisCommand {

  /** ZRANDMEMBER key [count [WITHSCORES]] */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    boolean withScores = false;
    int count = 1;
    boolean countSpecified = false;
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs < 2 || numArgs > 4) {
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
      if (numArgs > 2) {
        int countSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        long countPtr = inDataPtr;
        count = (int) Utils.strToLong(countPtr, countSize);
        countSpecified = true;
        inDataPtr += countSize;
      }

      if (numArgs > 3) {
        int valSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        long valPtr = inDataPtr;
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
              ZSets.ZRANDMEMBER(
                  map, keyPtr, keySize, count, withScores, outBufferPtr + off, outBufferSize - off);
      if (!countSpecified) {
        // WITHSCORES = false if count is not specified
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());
        // Update length of a field (- SIZEOF_DOUBLE)
        int len = Utils.readUVInt(outBufferPtr + off + Utils.SIZEOF_INT);
        int lenSize = Utils.sizeUVInt(len);
        // Copy field
        UnsafeAccess.copy(
            outBufferPtr + off + Utils.SIZEOF_INT + lenSize + Utils.SIZEOF_DOUBLE,
            outBufferPtr + off,
            len - Utils.SIZEOF_DOUBLE);
        UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, len - Utils.SIZEOF_DOUBLE);
        return;
      } else if (withScores) {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ZARRAY.ordinal());
      } else {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ZARRAY1.ordinal());
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
