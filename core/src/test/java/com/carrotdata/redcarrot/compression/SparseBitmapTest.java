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
package com.carrotdata.redcarrot.compression;

import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import org.junit.Ignore;
import org.junit.Test;

public class SparseBitmapTest {

  private static final Logger log = LogManager.getLogger(SparseBitmapTest.class);

  int size = 4096;

  // @Ignore
  @Test
  public void runTest() {
    CodecFactory factory = CodecFactory.getInstance();
    Codec codec = factory.getCodec(CodecType.LZ4);
    long src = UnsafeAccess.mallocZeroed(size);
    long dst = UnsafeAccess.mallocZeroed(2 * size);

    double pct = 0;
    Random r = new Random();
    for (int i = 1; i <= 100; i++) {
      pct += 0.01;
      UnsafeAccess.setMemory(src, size, (byte) 0);
      fill(src, pct, r);
      int compressedSize = codec.compress(src, size, dst, 2 * size);
      log.debug("Sparsiness {}% comp ratio={}", i, (((float) size) / compressedSize));
    }
  }

  @Ignore
  @Test
  public void runOneByteCompressionLZ4() {
    CodecFactory factory = CodecFactory.getInstance();
    Codec codec = factory.getCodec(CodecType.LZ4);
    long src = UnsafeAccess.mallocZeroed(size);
    long dst = UnsafeAccess.mallocZeroed(2 * size);
    Random r = new Random();

    for (int i = 0; i < size / 4; i++) {
      int v = r.nextInt(256);
      UnsafeAccess.putInt(src + i * 4, v);
    }

    int compressedSize = codec.compress(src, size, dst, 2 * size);
    log.debug("LZ4 1-byte compression ratio={}", (((float) size) / compressedSize));
  }

  @Ignore
  @Test
  public void runTwoBytesCompressionLZ4() {
    CodecFactory factory = CodecFactory.getInstance();
    Codec codec = factory.getCodec(CodecType.LZ4);
    long src = UnsafeAccess.mallocZeroed(size);
    long dst = UnsafeAccess.mallocZeroed(2 * size);
    Random r = new Random();

    for (int i = 0; i < size / 4; i++) {
      int v = r.nextInt(1000);
      UnsafeAccess.putInt(src + i * 4, v);
    }

    int compressedSize = codec.compress(src, size, dst, 2 * size);
    log.debug("LZ4 2-byte compression ratio={}", (((float) size) / compressedSize));
  }

  @Ignore
  @Test
  public void runOneByteCompressionLZ4HC() {
    CodecFactory factory = CodecFactory.getInstance();
    Codec codec = factory.getCodec(CodecType.LZ4HC);
    codec.setLevel(3);
    long src = UnsafeAccess.mallocZeroed(size);
    long dst = UnsafeAccess.mallocZeroed(2 * size);
    Random r = new Random();

    for (int i = 0; i < size / 4; i++) {
      int v = r.nextInt(256);
      UnsafeAccess.putInt(src + i * 4, v);
    }

    int compressedSize = codec.compress(src, size, dst, 2 * size);
    log.debug("LZ4HC 1-byte compression ratio={}", (((float) size) / compressedSize));
  }

  @Ignore
  @Test
  public void runTwoBytesCompressionLZ4HC() {
    CodecFactory factory = CodecFactory.getInstance();
    Codec codec = factory.getCodec(CodecType.LZ4HC);
    codec.setLevel(3);

    long src = UnsafeAccess.mallocZeroed(size);
    long dst = UnsafeAccess.mallocZeroed(2 * size);
    Random r = new Random();

    for (int i = 0; i < size / 4; i++) {
      int v = r.nextInt(1000);
      UnsafeAccess.putInt(src + i * 4, v);
    }

    int compressedSize = codec.compress(src, size, dst, 2 * size);
    log.debug("LZ4HC 2-byte compression ratio={}", (float) size / compressedSize);
  }

  private void fill(long src, double pct, Random r) {
    // TODO Auto-generated method stub
    int max = size * 8;
    for (int i = 0; i < max; i++) {
      double d = r.nextDouble();
      if (d < pct) {
        setBit(src, i);
      }
    }
  }

  private void setBit(long src, long offset) {
    int n = (int) (offset / 8);
    int pos = (int) (offset - n * 8);
    byte b = UnsafeAccess.toByte(src + n);
    b |= 1 << (7 - pos);
    UnsafeAccess.putByte(src + n, b);
  }
}
