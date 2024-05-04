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
package org.bigbase.carrot.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Not sure if we need this. Codec must be 
 *
 */
public class ZstdCodec implements Codec {

  private static final Logger log = LogManager.getLogger(ZstdCodec.class);

  /** The min comp size. */
  private int minCompSize = 100;

  /** The total size. */
  private AtomicLong totalSize = new AtomicLong();

  /** The total comp size. */
  private AtomicLong totalCompSize = new AtomicLong();

  /** The compression level. */
  private int level = 3;
  
  /** Dictionary size */
  private int dictSize = 1 << 16;// 64KB 
  
  @Override
  public int compress(ByteBuffer src, ByteBuffer dst) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int compress(long src, int srcSize, long dst, int dstCapacity) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int decompress(long src, int srcSize, long dst, int dstCapacity) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getCompressionThreshold() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setCompressionThreshold(int val) {
    // TODO Auto-generated method stub

  }

  @Override
  public CodecType getType() {
    return CodecType.ZSTD;
  }

  @Override
  public double getAvgCompressionRatio() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getTotalBytesProcessed() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setLevel(int level) {
    // TODO Auto-generated method stub

  }

  @Override
  public int getLevel() {
    // TODO Auto-generated method stub
    return 0;
  }

}
