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
package com.carrotdata.redcarrot.util;

import com.carrotdata.redcarrot.util.UnsafeAccess.Platform;
import static com.carrotdata.redcarrot.util.UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
import static com.carrotdata.redcarrot.util.UnsafeAccess.unsafeCopy;
import static com.carrotdata.redcarrot.util.UnsafeAccess.theUnsafe;
import static com.carrotdata.redcarrot.util.UnsafeAccess.UNSAFE_COPY_THRESHOLD;

public final class LittleEndianPlatform implements Platform {

  LittleEndianPlatform() {
  }

  @Override
  public void copy(byte[] src, int srcOffset, long address, int length) {
    Object destBase = null;
    long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(src, srcAddress, destBase, address, length);
  }

  @Override
  public void copy(long src, byte[] dest, int off, int length) {
    Object srcBase = null;
    long dstOffset = off + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(srcBase, src, dest, dstOffset, length);
  }

  @Override
  public void copy(long src, long dst, long len) {
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, dst, size);
      len -= size;
      src += size;
      dst += size;
    }
  }

  @Override
  public void copy_no_dst_check(long src, long dst, long len) {
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, dst, size);
      len -= size;
      src += size;
      dst += size;
    }
  }

  @Override
  public void copy_no_src_check(long src, long dst, long len) {
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, dst, size);
      len -= size;
      src += size;
      dst += size;
    }
  }

  @Override
  public void putByte(long addr, byte val) {
    theUnsafe.putByte(addr, val);
  }

  @Override
  public void putShort(long addr, short val) {
    val = Short.reverseBytes(val);
    theUnsafe.putShort(addr, val);
  }

  @Override
  public int putShort(byte[] bytes, int offset, short val) {
    val = Short.reverseBytes(val);
    theUnsafe.putShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_SHORT;
  }

  @Override
  public void putInt(long addr, int val) {
    val = Integer.reverseBytes(val);
    theUnsafe.putInt(addr, val);
  }

  @Override
  public int putInt(byte[] bytes, int offset, int val) {
    val = Integer.reverseBytes(val);
    theUnsafe.putInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_INT;
  }

  @Override
  public void putLong(long addr, long val) {
    val = Long.reverseBytes(val);
    theUnsafe.putLong(addr, val);
  }

  @Override
  public int putLong(byte[] bytes, int offset, long val) {
    val = Long.reverseBytes(val);
    theUnsafe.putLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_LONG;
  }

  @Override
  public byte toByte(long addr) {
    return theUnsafe.getByte(addr);
  }

  @Override
  public short toShort(long addr) {
    return Short.reverseBytes(theUnsafe.getShort(addr));
  }

  @Override
  public short toShort(byte[] bytes, int offset) {
    return Short.reverseBytes(theUnsafe.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
  }

  @Override
  public int toInt(long addr) {
    return Integer.reverseBytes(theUnsafe.getInt(addr));
  }

  @Override
  public int toInt(byte[] bytes, int offset) {
    return Integer.reverseBytes(theUnsafe.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
  }

  @Override
  public long toLong(long addr) {
    return Long.reverseBytes(theUnsafe.getLong(addr));
  }

  @Override
  public long toLong(byte[] bytes, int offset) {
    return Long.reverseBytes(theUnsafe.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
  }

  @Override
  public Platform getDebugVersion() {
    return new LittleEndianPlatformDebug();
  }

  @Override
  public Platform getNormalVersion() {
    return this;
  }

}
