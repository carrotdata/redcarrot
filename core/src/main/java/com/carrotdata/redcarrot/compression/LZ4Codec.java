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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.carrotdata.compression.lz4.LZ4;

// TODO: Auto-generated Javadoc
/** The Class LZ4Codec. */
public class LZ4Codec implements Codec {

  private static final Logger log = LogManager.getLogger(LZ4Codec.class);

  /** The min comp size. */
  private int minCompSize = 100;

  /** The total size. */
  private AtomicLong totalSize = new AtomicLong();

  /** The total comp size. */
  private AtomicLong totalCompSize = new AtomicLong();

  /** The level. */
  private int level = 1;

  /** Instantiates a new lz4 codec. */
  public LZ4Codec() {
    minCompSize = Integer.parseInt(System.getProperty(COMPRESSION_THRESHOLD, "100"));
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#compress(java.nio.ByteBuffer, java.nio.ByteBuffer)
   */
  @Override
  public int compress(ByteBuffer src, ByteBuffer dst) throws IOException {

    this.totalSize.addAndGet(src.limit() - src.position());
    int total = LZ4.compress(src, dst);
    this.totalCompSize.addAndGet(total);
    return total;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#decompress(java.nio.ByteBuffer, java.nio.ByteBuffer)
   */
  @Override
  public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException {

    int total = LZ4.decompress(src, dst);
    return total;
  }

  @Override
  public int compress(long src, int srcSize, long dst, int dstCapacity) {
    return LZ4.compressDirectAddress(src, srcSize, dst, dstCapacity);
  }

  @Override
  public int decompress(long src, int compressedSize, long dst, int dstCapacity) {
    return LZ4.decompressDirectAddress(src, compressedSize, dst, dstCapacity);
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#getCompressionThreshold()
   */
  @Override
  public int getCompressionThreshold() {

    return minCompSize;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#getType()
   */
  @Override
  public CodecType getType() {
    return CodecType.LZ4;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#setCompressionThreshold(int)
   */
  @Override
  public void setCompressionThreshold(int val) {
    minCompSize = val;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#getAvgCompressionRatio()
   */
  @Override
  public double getAvgCompressionRatio() {
    if (totalCompSize.get() == 0) {
      return 1.d;
    } else {
      return ((double) totalSize.get()) / totalCompSize.get();
    }
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#getLevel()
   */
  @Override
  public int getLevel() {

    return level;
  }

  /*
   * (non-Javadoc)
   * @see com.koda.compression.Codec#setLevel(int)
   */
  @Override
  public void setLevel(int level) {
    this.level = level;
  }

  @Override
  public long getTotalBytesProcessed() {
    return totalSize.get();
  }

  public static void main(String[] args) throws IOException {

    String str =
        "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy"
            + "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy"
            + "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy"
            + "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy"
            + "teruyiuylo[piptuytrtyytytytyttryjtruyrktuyuyrktyrytrjytjyuyrkg.kyrtyytejyyteyuyrkuyutuyuyruyrukytuyrkuy";

    str += str;
    str += str;
    str += str;
    str += str;
    str += str;

    ByteBuffer src = ByteBuffer.allocateDirect(102400);
    ByteBuffer dst = ByteBuffer.allocateDirect(102400);
    Codec codec = new LZ4Codec();

    byte[] buf = str.getBytes();
    src.put(buf);
    src.flip();
    int compSize = codec.compress(src, dst);
    log.debug("Size={} compressed ={}", str.length(), compSize);

    src.clear();

    int decSize = codec.decompress(dst, src);
    log.debug("Size={} decompressed ={}", str.length(), decSize);
  }
}
