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

// TODO: Auto-generated Javadoc
/** The Enum CodecType. */
public enum CodecType {

  /** No compression. */
  NONE(0),
  /** LZ4 */
  LZ4(1),
  /** LZ4-HC */
  LZ4HC(2),
  /** Bitmap codec */
  BITMAP(3),
  /** ZSTD */
  ZSTD(4);

  /** The id. */
  private int id;

  /**
   * Instantiates a new codec type.
   * @param id the id
   */
  private CodecType(int id) {
    this.id = id;
  }

  /**
   * Id.
   * @return the int
   */
  public int id() {
    return id;
  }

  /**
   * Gets the codec.
   * @return the codec
   */
  public Codec getCodec() {
    switch (id) {
      case 0:
        return CodecFactory.getInstance().getCodec(CodecType.NONE);
      case 1:
        return CodecFactory.getInstance().getCodec(CodecType.LZ4);
      case 2:
        return CodecFactory.getInstance().getCodec(CodecType.LZ4HC);
      case 4:
        return CodecFactory.getInstance().getCodec(CodecType.ZSTD);

    }
    return null;
  }
}
