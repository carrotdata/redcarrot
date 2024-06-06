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
package com.carrotdata.redcarrot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.compression.CodecFactory;
import com.carrotdata.redcarrot.compression.CodecType;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;
import org.junit.Test;

import static org.junit.Assert.*;

public class DataBlockScannerTest {

  private static final Logger log = LogManager.getLogger(DataBlockScannerTest.class);

  protected DataBlock getDataBlock() {
    IndexBlock ib = new IndexBlock(null, 4096);
    ib.setFirstIndexBlock();
    ib.firstBlock();
    return ib.firstBlock();
  }

  @Test
  public void testFullScan() throws IOException {
    log.debug("testFullScan");
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    loadedKeyDebug(keys.size());
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip first system key
    assertNotNull(scanner);
    scanner.next();
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
  }

  @Test
  public void testFullScanCompressionDecompression() throws IOException {
    log.debug("testFullScanCompressionDecompression");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));

    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    loadedKeyDebug(keys.size());
    ib.compressDataBlockIfNeeded();
    ib.decompressDataBlockIfNeeded();
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip first system key
    assertNotNull(scanner);
    scanner.next();
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }

  @Test
  public void testFullScanReverseCompressionDecompression() throws IOException {
    log.debug("testFullScanReverseCompressionDecompression");
    // Enable data block compression
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));

    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    loadedKeyDebug(keys.size());
    ib.compressDataBlockIfNeeded();
    ib.decompressDataBlockIfNeeded();
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip first system key
    assertNotNull(scanner);
    scanner.last();
    verifyScannerReverse(scanner, keys);
    scanner.close();
    dispose(keys);
    // Disable data block compression
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }

  @Test
  public void testFullScanReverse() throws IOException {
    log.debug("testFullScanReverse");
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    loadedKeyDebug(keys.size());
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip first system key
    assertNotNull(scanner);
    scanner.last();
    verifyScannerReverse(scanner, keys);
    scanner.close();
    dispose(keys);
  }

  private void dispose(List<Key> keys) {
    for (Key key : keys) {
      UnsafeAccess.free(key.address);
    }
  }

  @Test
  public void testOpenStartScan() throws IOException {
    log.debug("testOpenStartScan");
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int stopRowIndex = r.nextInt(keys.size());
    Key stopRow = keys.get(stopRowIndex);
    loadedKeyDebug(keys.size());
    keys = keys.subList(0, stopRowIndex);
    log.debug("Selected {} kvs", keys.size());
    DataBlockScanner scanner =
        DataBlockScanner.getScanner(ib, 0, 0, stopRow.address, stopRow.length, Long.MAX_VALUE);
    // Skip first system key
    assertNotNull(scanner);
    scanner.next();
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
  }

  @Test
  public void testOpenStartScanReverse() throws IOException {
    log.debug("testOpenStartScanReverse");
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("testOpenStartScanReverse seed={}", seed);
    int stopRowIndex = r.nextInt(keys.size());
    Key stopRow = keys.get(stopRowIndex);
    loadedKeyDebug(keys.size());
    keys = keys.subList(0, stopRowIndex);
    log.debug("Selected {} kvs", keys.size());
    if (keys.size() == 0) {
      log.debug("");
    }
    DataBlockScanner scanner =
        DataBlockScanner.getScanner(ib, 0, 0, stopRow.address, stopRow.length, Long.MAX_VALUE);
    if (scanner != null) {
      if (scanner.last()) {
        verifyScannerReverse(scanner, keys);
      } else {
        assertEquals(0, keys.size());
      }
      scanner.close();
    } else {
      assertEquals(0, keys.size());
    }
    dispose(keys);
  }

  @Test
  public void testOpenEndScan() throws IOException {
    log.debug("testOpenEndScan");
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    Key startRow = keys.get(startRowIndex);
    loadedKeyDebug(keys.size());
    keys = keys.subList(startRowIndex, keys.size());
    log.debug("Selected {} kvs", keys.size());
    DataBlockScanner scanner =
        DataBlockScanner.getScanner(ib, startRow.address, startRow.length, 0, 0, Long.MAX_VALUE);
    assertNotNull(scanner);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
  }

  @Test
  public void testOpenEndScanReverse() throws IOException {
    log.debug("testOpenEndScanReverse");
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("testOpenEndScanReverse seed={}", seed);
    int startRowIndex = r.nextInt(keys.size());
    Key startRow = keys.get(startRowIndex);
    loadedKeyDebug(keys.size());
    keys = keys.subList(startRowIndex, keys.size());
    log.debug("Selected {} kvs", keys.size());
    DataBlockScanner scanner =
        DataBlockScanner.getScanner(ib, startRow.address, startRow.length, 0, 0, Long.MAX_VALUE);
    if (scanner != null) {
      if (scanner.last()) {
        verifyScannerReverse(scanner, keys);
      } else {
        assertEquals(0, keys.size());
      }
      scanner.close();
    } else {
      assertEquals(0, keys.size());
    }
    dispose(keys);
  }

  @Test
  public void testSubScan() throws IOException {
    log.debug("testSubScan");
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size() - startRowIndex) + 1 + startRowIndex;
    if (stopRowIndex >= keys.size()) {
      stopRowIndex = keys.size() - 1;
    }
    Key startRow = keys.get(startRowIndex);
    Key stopRow = keys.get(stopRowIndex);
    loadedKeyDebug(keys.size());
    keys = keys.subList(startRowIndex, stopRowIndex);
    log.debug("Selected {} kvs", keys.size());
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, startRow.address, startRow.length,
      stopRow.address, stopRow.length, Long.MAX_VALUE);
    if (scanner != null) {
      verifyScanner(scanner, keys);
      scanner.close();
    } else {
      assertEquals(0, keys.size());
    }
    dispose(keys);
  }

  @Test
  public void testSubScanReverse() throws IOException {
    log.debug("testSubScanReverse");
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("testSubScanReverse seed={}", seed);
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size() - startRowIndex) + 1 + startRowIndex;
    if (stopRowIndex >= keys.size()) {
      stopRowIndex = keys.size() - 1;
    }
    Key startRow = keys.get(startRowIndex);
    Key stopRow = keys.get(stopRowIndex);
    loadedKeyDebug(keys.size());
    keys = keys.subList(startRowIndex, stopRowIndex);

    log.debug("Selected {} kvs", keys.size());
    // When start and stop rows are equals
    // scanner must be null
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, startRow.address, startRow.length,
      stopRow.address, stopRow.length, Long.MAX_VALUE);
    if (scanner != null) {
      if (scanner.last()) {
        verifyScannerReverse(scanner, keys);
      } else {
        assertEquals(0, keys.size());
      }
      scanner.close();
    } else {
      assertEquals(0, keys.size());
    }
    dispose(keys);
  }

  private void verifyScanner(DataBlockScanner scanner, List<Key> keys) {
    int count = 0;

    while (scanner.hasNext()) {
      count++;
      Key key = keys.get(count - 1);
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      assertEquals(key.length, keySize);
      assertEquals(key.length, valSize);
      byte[] buf = new byte[keySize];
      scanner.key(buf, 0);
      assertEquals(0, Utils.compareTo(buf, 0, buf.length, key.address, key.length));
      scanner.value(buf, 0);
      assertEquals(0, Utils.compareTo(buf, 0, buf.length, key.address, key.length));
      scanner.next();
    }

    assertEquals(keys.size(), count);
  }

  private void verifyScannerReverse(DataBlockScanner scanner, List<Key> keys) {
    int count = 0;

    Collections.reverse(keys);
    do {
      count++;
      Key key = keys.get(count - 1);
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      assertEquals(key.length, keySize);
      assertEquals(key.length, valSize);
      byte[] buf = new byte[keySize];
      scanner.key(buf, 0);
      assertEquals(0, Utils.compareTo(buf, 0, buf.length, key.address, key.length));
      scanner.value(buf, 0);
      assertEquals(0, Utils.compareTo(buf, 0, buf.length, key.address, key.length));
    } while (scanner.previous());

    Collections.reverse(keys);

    assertEquals(keys.size(), count);
  }

  protected ArrayList<Key> fillDataBlock(DataBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Fill seed={}", seed);
    int length = 32;
    boolean result = true;
    while (result) {
      byte[] key = new byte[length];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(length);
      UnsafeAccess.copy(key, 0, ptr, length);
      result = b.put(ptr, length, ptr, length, 0, 0);
      if (result) {
        keys.add(new Key(ptr, length));
      }
    }
    log.debug("{} {}", b.getNumberOfRecords(), b.getDataInBlockSize());
    return keys;
  }

  private void loadedKeyDebug(int keysize) {
    log.debug("Loaded {} kvs", keysize);
  }
}
