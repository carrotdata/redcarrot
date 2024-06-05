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

import com.carrotdata.redcarrot.redis.commands.RedisCommand.ReplyType;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class Errors {

  static final byte[] TYPE_GENERIC = new byte[] { (byte) 'E', (byte) 'R', (byte) 'R' };
  static final byte[] SPACE = new byte[] { (byte) ':', (byte) ' ' };
  static final byte[] ERR_WRONG_NUMBER_FORMAT = "Wrong number format".getBytes();
  static final byte[] ERR_WRONG_BIT_VALUE = "Wrong bit value (must be 0 or 1)".getBytes();
  static final byte[] ERR_WRONG_ARGS_NUMBER = "Wrong number of arguments".getBytes();
  static final byte[] ERR_WRONG_COMMAND_FORMAT =
      "Wrong command format, unexpected argument".getBytes();
  static final byte[] ERR_KEY_DOESNOT_EXIST = "Key does not exist".getBytes();
  static final byte[] ERR_OPERATION_FAILED = "Operation failed".getBytes();
  static final byte[] ERR_KEY_NOT_NUMBER = "Value at key is not a number".getBytes();
  static final byte[] ERR_INVALID_CURSOR = "Invalid cursor".getBytes();
  static final byte[] ERR_OUT_OF_RANGE = "Index is out of range".getBytes();
  static final byte[] ERR_OUT_OF_RANGE_OR =
      "Index is out of range or key does not exist".getBytes();
  static final byte[] ERR_POSITIVE_NUMBER_EXPECTED = "Positive number expected".getBytes();
  static final String ERR_MIN_SPECIFIED =
      "Either '(' or '[' or '-' can be specified for a min argument";
  static final String ERR_MAX_SPECIFIED =
      "Either '(' or '[' or '+' can be specified for a max argument";
  static final byte[] ERR_UNSUPPORTED_COMMAND = "Unsupported command".getBytes();
  static final byte[] ERR_SNAPSHOT_RUNNING = "Snapshot is already running".getBytes();

  public static void write(long buffer, byte[] type, byte[] message) {
    int off = 0;
    UnsafeAccess.putByte(buffer, (byte) ReplyType.ERROR.ordinal());
    off += Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
    UnsafeAccess.copy(type, 0, buffer + off, type.length);
    off += type.length;
    UnsafeAccess.copy(SPACE, 0, buffer + off, SPACE.length);
    off += SPACE.length;
    UnsafeAccess.copy(message, 0, buffer + off, message.length);
    off += message.length;
    // Set the message's length
    UnsafeAccess.putInt(buffer + Utils.SIZEOF_BYTE, off - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);
  }

  public static void write(long buffer, byte[] type, byte[] message, String reason) {
    int off = 0;
    UnsafeAccess.putByte(buffer, (byte) ReplyType.ERROR.ordinal());
    off += Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
    UnsafeAccess.copy(type, 0, buffer + off, type.length);
    off += type.length;
    UnsafeAccess.copy(SPACE, 0, buffer + off, SPACE.length);
    off += SPACE.length;
    UnsafeAccess.copy(message, 0, buffer + off, message.length);
    off += message.length;
    if (reason != null) {
      byte[] bytes = reason.getBytes();
      UnsafeAccess.copy(bytes, 0, buffer + off, bytes.length);
      off += bytes.length;
    }
    // Set the message's length
    UnsafeAccess.putInt(buffer + Utils.SIZEOF_BYTE, off - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);
  }

  public static void write(long buffer, byte[] type, byte[] message, byte[] reason) {
    int off = 0;
    UnsafeAccess.putByte(buffer, (byte) ReplyType.ERROR.ordinal());
    off += Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
    UnsafeAccess.copy(type, 0, buffer + off, type.length);
    off += type.length;
    UnsafeAccess.copy(SPACE, 0, buffer + off, SPACE.length);
    off += SPACE.length;
    UnsafeAccess.copy(message, 0, buffer + off, message.length);
    off += message.length;
    if (reason != null) {
      byte[] bytes = reason;
      UnsafeAccess.copy(bytes, 0, buffer + off, bytes.length);
      off += bytes.length;
    }
    // Set the message's length
    UnsafeAccess.putInt(buffer + Utils.SIZEOF_BYTE, off - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);
  }
}
