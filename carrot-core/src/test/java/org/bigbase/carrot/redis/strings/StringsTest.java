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
package org.bigbase.carrot.redis.strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.CarrotCoreBase;
import org.bigbase.carrot.ops.OperationFailedException;
import org.bigbase.carrot.redis.util.Commons;
import org.bigbase.carrot.redis.util.MutationOptions;
import org.bigbase.carrot.util.KeyValue;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class StringsTest extends CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(StringsTest.class);

  private long buffer;
  private final int bufferSize = 512;
  private List<KeyValue> keyValues;

  public StringsTest(Object c) {
    super(c);
  }

  private List<KeyValue> getKeyValues() {
    List<KeyValue> keyValues = new ArrayList<>();
    for (int i = 0; i < nKeyValues; i++) {
      // key
      byte[] key = ("user:" + i).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      int keySize = key.length;
      //      log.debug("copy key: {}, keyPtr: {}, keySize: {}", new String(key), keyPtr, keySize);
      UnsafeAccess.copy(key, 0, keyPtr, keySize);

      // value
      FakeUserSession session = FakeUserSession.newSession(i);
      byte[] value = session.toString().getBytes();
      int valueSize = value.length;
      long valuePtr = UnsafeAccess.malloc(valueSize);
      //      log.debug(
      //          "copy value: {}, valuePtr: {}, valueSize: {}", new String(value), valuePtr,
      // valueSize);
      UnsafeAccess.copy(value, 0, valuePtr, valueSize);
      keyValues.add(new KeyValue(keyPtr, keySize, valuePtr, valueSize));
    }
    return keyValues;
  }

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();

    buffer = UnsafeAccess.mallocZeroed(bufferSize);
    keyValues = getKeyValues();
  }

  @Test
  public void testGetExpire() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);

    Random r = new Random();
    long exp = 0;

    boolean res =
        Strings.SET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp,
            MutationOptions.NONE,
            false);
    assertTrue(res);
    long size = Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    assertEquals(kv.valueSize, (int) size);
    assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, (int) size));

    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);

    exp = Math.abs(r.nextLong());
    res =
        Strings.SET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp,
            MutationOptions.NONE,
            false);
    assertTrue(res);

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);

    exp = Math.abs(r.nextLong());
    res =
        Strings.SET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp,
            MutationOptions.NONE,
            false);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);

    assertEquals(exp, expire);

    exp = Math.abs(r.nextLong());
    res =
        Strings.SET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp,
            MutationOptions.NONE,
            false);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);

    assertEquals(exp, expire);

    exp = Math.abs(r.nextLong());
    res =
        Strings.SET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp,
            MutationOptions.NONE,
            false);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);

    assertEquals(exp, expire);
  }

  @Test
  public void testSetIfNotExists() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);
    long exp = 10;

    boolean res = Strings.SETNX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertTrue(res);

    res = Strings.SETNX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertFalse(res);

    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
  }

  @Test
  public void testSetIfExists() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);
    long exp = 10;

    boolean res = Strings.SETXX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertFalse(res);

    res = Strings.SETNX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertTrue(res);

    exp = 100;
    res = Strings.SETXX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertTrue(res);

    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
  }

  @Test
  public void testSetWithTTL() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);
    long exp = 10;

    boolean res =
        Strings.SET(
            map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, true);
    assertTrue(res);

    res =
        Strings.SET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp + 10,
            MutationOptions.NONE,
            true);
    assertTrue(res);

    // Check that expire did not change
    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);

    res = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(res);

    res =
        Strings.SET(
            map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.XX, true);
    assertFalse(res);

    res =
        Strings.SET(
            map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, true);
    assertTrue(res);

    exp += 10;
    res =
        Strings.SET(
            map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.XX, false);
    assertTrue(res);

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);

    res =
        Strings.SET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp + 10,
            MutationOptions.XX,
            true);
    assertTrue(res);

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);

    res =
        Strings.SET(
            map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NX, true);
    assertFalse(res);

    res = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(res);

    res =
        Strings.SET(
            map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NX, true);
    assertTrue(res);

    exp += 10;
    res =
        Strings.SET(
            map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.XX, false);
    assertTrue(res);

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);

    res =
        Strings.SET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp + 10,
            MutationOptions.XX,
            true);
    assertTrue(res);

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
  }

  @Test
  public void testSetGetWithTTL() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);
    KeyValue kv1 = keyValues.get(1);
    KeyValue kv2 = keyValues.get(2);
    long exp = 10;
    // long SET_FAILED = -2;
    long GET_FAILED = -1;

    long res =
        Strings.SETGET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp,
            MutationOptions.NONE,
            true,
            buffer,
            bufferSize);
    assertEquals(GET_FAILED, res);

    res =
        Strings.SETGET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp + 10,
            MutationOptions.NONE,
            true,
            buffer,
            bufferSize);
    assertEquals(kv.valueSize, (int) res);
    assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, (int) res));

    // Check that expire did not change
    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);

    boolean result = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(result);

    res =
        Strings.SETGET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp,
            MutationOptions.XX,
            true,
            buffer,
            bufferSize);
    assertEquals(GET_FAILED, res);

    result =
        Strings.SET(
            map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, true);
    assertTrue(result);

    exp += 10;
    res =
        Strings.SETGET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv1.valuePtr,
            kv1.valueSize,
            exp,
            MutationOptions.XX,
            false,
            buffer,
            bufferSize);
    assertEquals(kv.valueSize, (int) res);

    assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, (int) res));

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);

    res =
        Strings.SETGET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp + 10,
            MutationOptions.XX,
            true,
            buffer,
            bufferSize);
    assertEquals(kv1.valueSize, (int) res);
    assertEquals(0, Utils.compareTo(kv1.valuePtr, kv1.valueSize, buffer, (int) res));

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);

    res =
        Strings.SETGET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp,
            MutationOptions.NX,
            true,
            buffer,
            bufferSize);
    assertEquals(kv.valueSize, (int) res);
    assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, (int) res));

    result = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(result);

    res =
        Strings.SETGET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv2.valuePtr,
            kv2.valueSize,
            exp,
            MutationOptions.NX,
            true,
            buffer,
            bufferSize);
    assertEquals(GET_FAILED, res);

    exp += 10;
    res =
        Strings.SETGET(
            map,
            kv.keyPtr,
            kv.keySize,
            kv.valuePtr,
            kv.valueSize,
            exp,
            MutationOptions.XX,
            false,
            buffer,
            bufferSize);
    assertEquals(kv2.valueSize, (int) res);

    assertEquals(0, Utils.compareTo(kv2.valuePtr, kv2.valueSize, buffer, (int) res));

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
  }

  @Test
  public void testIncrementLongWrongFormat() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);
    Strings.SET(
        map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 0, MutationOptions.NONE, false);
    try {
      Strings.INCRBY(map, kv.keyPtr, kv.keySize, 1);
    } catch (OperationFailedException e) {
      return;
    }
    fail("Test failed");
  }

  @Test
  public void testIncrementDoubleWrongFormat() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);
    Strings.SET(
        map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 0, MutationOptions.NONE, false);
    try {
      Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 1d);
    } catch (OperationFailedException e) {
      return;
    }
    fail("Test failed");
  }

  @Test
  public void testIncrementLong() throws OperationFailedException {
    log.debug(getTestParameters());

    KeyValue kv = keyValues.get(0);

    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 0);
    int size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    long value = Utils.strToLong(buffer, size);
    assertEquals(0L, value);

    Strings.INCRBY(map, kv.keyPtr, kv.keySize, -11110);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    String svalue = Utils.toString(buffer, size);
    assertEquals(-11110L, value);

    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 11110);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(0L, value);

    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 10);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(10L, value);

    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 100);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(110L, value);

    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 1000);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(1110L, value);

    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 10000);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(11110L, value);

    Strings.INCRBY(map, kv.keyPtr, kv.keySize, Long.MIN_VALUE);
    long newValue = value + Long.MIN_VALUE;
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);

    assertEquals(newValue, value);
  }

  @Test
  public void testIncrementDouble() throws OperationFailedException {
    log.debug(getTestParameters());

    KeyValue kv = keyValues.get(0);

    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 10d);

    int size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    double value = Utils.strToDouble(buffer, size);
    assertEquals(10d, value, 0.0);

    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 100d);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(110d, value, 0.0);

    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 1000d);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(1110d, value, 0.0);

    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 10000d);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(11110d, value, 0.0);

    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, -11110d);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(0d, value, 0.0);

    double incr = Double.MAX_VALUE / 2;
    double newValue = value + incr;

    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, incr);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(newValue, value, 0.0);

    incr = -Double.MAX_VALUE / 2;
    newValue = value + incr;

    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, incr);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(newValue, value, 0.0);
  }

  @Test
  public void testSetGet() {
    log.debug(getTestParameters());

    long start = System.currentTimeMillis();
    long totalSize = 0;
    for (int i = 0; i < nKeyValues; i++) {
      KeyValue kv = keyValues.get(i);
      totalSize += kv.keySize + kv.valueSize;
      boolean result =
          Strings.SET(
              map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 0, MutationOptions.NONE, true);
      assertTrue(result);
      if ((i + 1) % 10000 == 0) {
        log.debug(i + 1);
      }
    }
    long end = System.currentTimeMillis();
    log.debug(
        "Total allocated memory ={} for {} {} byte values. Overhead={} bytes per key-value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        nKeyValues,
        totalSize,
        (double) BigSortedMap.getGlobalAllocatedMemory() - totalSize / nKeyValues,
        end - start);
    start = System.currentTimeMillis();
    for (int i = 0; i < nKeyValues; i++) {
      KeyValue kv = keyValues.get(i);
      long valueSize = Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
      assertEquals(kv.valueSize, (int) valueSize);
      assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, (int) valueSize));
    }
    end = System.currentTimeMillis();
    log.debug("Time GET ={}ms", end - start);
    BigSortedMap.printGlobalMemoryAllocationStats();
  }

  @Test
  public void testSetRemove() {
    log.debug(getTestParameters());

    long start = System.currentTimeMillis();
    long totalSize = 0;
    for (int i = 0; i < nKeyValues; i++) {
      KeyValue kv = keyValues.get(i);
      totalSize += kv.keySize + kv.valueSize;
      boolean result =
          Strings.SET(
              map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 0, MutationOptions.NONE, true);
      assertTrue(result);
      if ((i + 1) % 10000 == 0) {
        log.debug(i + 1);
      }
    }

    long end = System.currentTimeMillis();
    log.debug(
        "Total allocated memory ={} for {} {}  byte values. Overhead={} bytes per key-value. Time to load: {}ms",
        BigSortedMap.getGlobalAllocatedMemory(),
        nKeyValues,
        totalSize,
        ((double) BigSortedMap.getGlobalAllocatedMemory() - totalSize) / nKeyValues,
        end - start);
    start = System.currentTimeMillis();
    for (int i = 0; i < nKeyValues; i++) {
      KeyValue kv = keyValues.get(i);
      boolean result = Strings.DELETE(map, kv.keyPtr, kv.keySize);
      assertTrue(result);
    }
    end = System.currentTimeMillis();
    log.debug("Time DELETE ={}ms", end - start);

    start = System.currentTimeMillis();
    for (int i = 0; i < nKeyValues; i++) {
      KeyValue kv = keyValues.get(i);
      long result = Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
      assertEquals(-1, (int) result);
    }
    end = System.currentTimeMillis();
    log.debug("Time to GET {} values={}ms", nKeyValues, end - start);
  }

  @Test
  public void testAppend() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);

    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(kv.valueSize, size);

    size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(2 * kv.valueSize, size);

    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);

    assertEquals(2 * kv.valueSize, size);
    assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, kv.valueSize));
    assertEquals(
        0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer + kv.valueSize, kv.valueSize));
  }

  @Test
  public void testGetDelete() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);

    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(kv.valueSize, size);

    size = Strings.GETDEL(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    assertEquals(kv.valueSize, size);
    assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, kv.valueSize));

    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    assertEquals(-1, size); // not found
    size = Strings.GETDEL(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    assertEquals(-1, size); // not found
  }

  @Test
  public void testGetEx() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);

    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(kv.valueSize, size);

    size = Strings.GETEX(map, kv.keyPtr, kv.keySize, 100, buffer, bufferSize);
    assertEquals(kv.valueSize, size);
    assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, kv.valueSize));

    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(100L, expire);
  }

  @Test
  public void testGetSet() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);
    KeyValue kv1 = keyValues.get(1);

    long size =
        Strings.GETSET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, buffer, bufferSize);
    assertEquals(-1L, size);

    size =
        Strings.GETSET(map, kv.keyPtr, kv.keySize, kv1.valuePtr, kv1.valueSize, buffer, bufferSize);
    assertEquals(kv.valueSize, (int) size);
    assertEquals(0, Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, kv.valueSize));

    size =
        Strings.GETSET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, buffer, bufferSize);
    assertEquals(kv1.valueSize, (int) size);
    assertEquals(0, Utils.compareTo(kv1.valuePtr, kv1.valueSize, buffer, kv1.valueSize));
  }

  @Test
  public void testStrLength() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);
    long size = Strings.STRLEN(map, kv.keyPtr, kv.keySize);
    assertEquals(0L, size);
    size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(kv.valueSize, (int) size);
    size = Strings.STRLEN(map, kv.keyPtr, kv.keySize);
    assertEquals(kv.valueSize, (int) size);
  }

  @Test
  public void testSetEx() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);

    boolean res = Strings.SETEX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 100);
    assertTrue(res);

    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(100L, expire);

    res = Strings.SETEX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 200);
    assertTrue(res);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(200L, expire);
  }

  @Test
  public void testPSetEx() {
    log.debug(getTestParameters());
    KeyValue kv = keyValues.get(0);

    boolean res = Strings.PSETEX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 100);
    assertTrue(res);

    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(100L, expire);

    res = Strings.PSETEX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 200);
    assertTrue(res);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(200L, expire);
  }

  @Test
  public void testMget() {
    log.debug(getTestParameters());
    for (KeyValue kv : keyValues) {
      // we use key as a value b/c its small
      Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.keyPtr, kv.keySize);
    }

    // Test existing
    for (int i = 0; i < 100; i++) {
      long[] arr = Utils.randomDistinctArray(keyValues.size() - 1, 11);
      long[] ptrs = new long[arr.length];
      int[] sizes = new int[arr.length];
      for (int k = 0; k < arr.length; k++) {
        KeyValue kv = keyValues.get((int) arr[k]);
        ptrs[k] = kv.keyPtr;
        sizes[k] = kv.keySize;
      }

      Strings.MGET(map, ptrs, sizes, buffer, bufferSize);
      verify(arr);
    }

    // Test run non-existent

    long[] arr = Utils.randomDistinctArray(keyValues.size() - 1, 11);
    long[] ptrs = new long[arr.length];
    int[] sizes = new int[arr.length];
    for (int k = 0; k < arr.length; k++) {
      KeyValue kv = keyValues.get((int) arr[k]);
      ptrs[k] = kv.valuePtr;
      sizes[k] = kv.valueSize;
    }

    long size = Strings.MGET(map, ptrs, sizes, buffer, bufferSize);
    assertEquals(Utils.SIZEOF_INT + (long) arr.length * Utils.SIZEOF_INT, (int) size);
    long ptr = buffer;
    assertEquals(arr.length, UnsafeAccess.toInt(ptr));
    ptr += Utils.SIZEOF_INT;
    for (int i = 0; i < arr.length; i++) {
      assertEquals(-1, UnsafeAccess.toInt(ptr));
      ptr += Utils.SIZEOF_INT;
    }
  }

  private void verify(long[] arr) {
    assertEquals(arr.length, UnsafeAccess.toInt(buffer));
    long ptr = buffer + Utils.SIZEOF_INT;
    for (long l : arr) {
      KeyValue kv = keyValues.get((int) l);
      assertEquals(kv.keySize, UnsafeAccess.toInt(ptr));
      ptr += Utils.SIZEOF_INT;
      assertEquals(0, Utils.compareTo(kv.keyPtr, kv.keySize, ptr, kv.keySize));
      ptr += kv.keySize;
    }
  }

  @Test
  public void testMSet() {
    log.debug(getTestParameters());
    Strings.MSET(map, keyValues);
    for (KeyValue kv : keyValues) {
      assertTrue(Strings.keyExists(map, kv.keyPtr, kv.keySize));
    }
  }

  @Test
  public void testMSetNX() {
    log.debug(getTestParameters());
    boolean res = Strings.MSETNX(map, keyValues);
    assertTrue(res);
    for (KeyValue kv : keyValues) {
      assertTrue(Strings.keyExists(map, kv.keyPtr, kv.keySize));
    }
    res = Strings.MSETNX(map, keyValues);
    assertFalse(res);
  }

  @Test
  public void testSetGetBit() {
    log.debug(getTestParameters());

    testSetGetBitInternal(1000);
    testSetGetBitInternal(10000);
  }

  private void testSetGetBitInternal(int valueSize) {
    // Small bitset test
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    KeyValue kv = keyValues.get(0);
    // Check non-existing key
    int bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, -1000);
    assertEquals(0, bit);
    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, 0);
    assertEquals(0, bit);
    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, 1000);
    assertEquals(0, bit);

    // Set K-V
    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    assertEquals(valueSize, size);

    // Check some bits - MUST be 0 all
    for (int i = 0; i < valueSize * Utils.BITS_PER_BYTE; i++) {
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, i);
      assertEquals(0, bit);
    }

    for (int i = 0; i < valueSize * Utils.BITS_PER_BYTE; i++) {
      bit = Strings.SETBIT(map, kv.keyPtr, kv.keySize, i, 1);
      assertEquals(0, bit);
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, i);
      assertEquals(1, bit);
      bit = Strings.SETBIT(map, kv.keyPtr, kv.keySize, i, 0);
      assertEquals(1, bit);
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, i);
      assertEquals(0, bit);
    }

    // Some out of range ops
    Strings.SETBIT(map, kv.keyPtr, kv.keySize, 2L * valueSize * Utils.BITS_PER_BYTE, 1);
    long len = Strings.STRLEN(map, kv.keyPtr, kv.keySize);
    assertEquals(2L * valueSize + 1, len);

    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, 2L * valueSize * Utils.BITS_PER_BYTE);
    assertEquals(1, bit);

    bit = Strings.SETBIT(map, kv.keyPtr, kv.keySize, 2L * valueSize * Utils.BITS_PER_BYTE, 0);
    assertEquals(1, bit);

    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, 2L * valueSize * Utils.BITS_PER_BYTE);
    assertEquals(0, bit);

    // Verify all 0s
    for (int i = 0; i < (2 * valueSize + 1) * Utils.BITS_PER_BYTE; i++) {
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, 2L * valueSize * Utils.BITS_PER_BYTE);
      assertEquals(0, bit);
    }

    boolean result = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(result);

    bit = Strings.SETBIT(map, kv.keyPtr, kv.keySize, 2L * valueSize * Utils.BITS_PER_BYTE, 1);
    assertEquals(0, bit);

    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, 2L * valueSize * Utils.BITS_PER_BYTE);
    assertEquals(1, bit);

    len = Strings.STRLEN(map, kv.keyPtr, kv.keySize);
    assertEquals(2L * valueSize + 1, len);

    for (int i = 0; i < 2 * valueSize * Utils.BITS_PER_BYTE; i++) {
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, i);
      assertEquals(0, bit);
    }

    Strings.DELETE(map, kv.keyPtr, kv.keySize);
    UnsafeAccess.free(valuePtr);
  }

  @Test
  public void testGetRange() {
    log.debug(getTestParameters());

    testGetRangeInternal(1000);
    testGetRangeInternal(10000);
  }

  private void testGetRangeInternal(int valueSize) {
    // Small bitset test
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    Utils.fillRandom(valuePtr, valueSize);

    KeyValue kv = keyValues.get(0);
    Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    long buf = UnsafeAccess.malloc(valueSize);
    int bufSize = valueSize;
    long buf1 = UnsafeAccess.malloc(valueSize);
    int bufSize1 = valueSize;

    // Tets edge cases
    // 1. end = start = +-inf
    int size =
        Strings.GETRANGE(
            map, kv.keyPtr, kv.keySize, Commons.NULL_LONG, Commons.NULL_LONG, buf, bufSize);
    assertEquals(valueSize, size);
    assertEquals(0, Utils.compareTo(valuePtr, valueSize, buf, valueSize));

    // 1. start = -inf, end is in range
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    for (int i = 0; i < 100; i++) {
      long start = Commons.NULL_LONG;
      long end = r.nextInt(valueSize);
      int expSize = getRange(valuePtr, valueSize, start, end, buf);
      size = Strings.GETRANGE(map, kv.keyPtr, kv.keySize, start, end, buf1, bufSize1);
      assertEquals(expSize, size);
      if (size > 0) {
        assertEquals(0, Utils.compareTo(buf, size, buf1, size));
      }
    }

    // 2. end = +inf, start is in range

    for (int i = 0; i < 100; i++) {
      long end = Commons.NULL_LONG;
      long start = r.nextInt(valueSize);
      int expSize = getRange(valuePtr, valueSize, start, end, buf);
      size = Strings.GETRANGE(map, kv.keyPtr, kv.keySize, start, end, buf1, bufSize1);
      assertEquals(expSize, size);
      if (size > 0) {
        assertEquals(0, Utils.compareTo(buf, size, buf1, size));
      }
    }

    // 3. end and start are in range

    for (int i = 0; i < 200; i++) {
      long end = r.nextInt(valueSize);
      long start = r.nextInt(valueSize);
      int expSize = getRange(valuePtr, valueSize, start, end, buf);
      size = Strings.GETRANGE(map, kv.keyPtr, kv.keySize, start, end, buf1, bufSize1);
      assertEquals(expSize, size);
      if (size > 0) {
        assertEquals(0, Utils.compareTo(buf, size, buf1, size));
      }
    }
    Strings.DELETE(map, kv.keyPtr, kv.keySize);
    UnsafeAccess.free(buf);
    UnsafeAccess.free(buf1);
    UnsafeAccess.free(valuePtr);
  }

  private int getRange(long ptr, int size, long start, long end, long buffer) {
    // We assume that bufferSize >= size
    if (start == Commons.NULL_LONG) {
      start = 0;
    }
    if (end == Commons.NULL_LONG) {
      end = size - 1;
    }
    if (start < 0) {
      start = start + size;
    }
    if (start < 0) {
      start = 0;
    }
    if (end < 0) {
      end = end + size;
    }
    if (end >= size) {
      end = size - 1;
    }
    if (end < 0 || end < start || start >= size) {
      return -1;
    }
    UnsafeAccess.copy(ptr + start, buffer, end - start + 1);
    return (int) (end - start + 1);
  }

  @Test
  public void testSetRange() {
    log.debug(getTestParameters());

    int valueSize = 1000;
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    Utils.fillRandom(valuePtr, valueSize);

    KeyValue kv = keyValues.get(0);
    Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    long buf = UnsafeAccess.malloc(valueSize); // To make sure that test won't fail
    int bufSize = valueSize;

    // Test edge cases
    // 1. offset < 0
    int size =
        (int)
            Strings.SETRANGE(
                map, kv.keyPtr, kv.keySize, Commons.NULL_LONG, kv.valuePtr, kv.valueSize);
    assertEquals(-1, size);

    for (int offset = 0; offset < 10000; offset++) {
      size = (int) Strings.SETRANGE(map, kv.keyPtr, kv.keySize, offset, kv.valuePtr, kv.valueSize);
      int expSize = Math.max(valueSize, offset + kv.valueSize);
      assertEquals(expSize, size);
      size =
          Strings.GETRANGE(
              map, kv.keyPtr, kv.keySize, offset, offset + kv.valueSize - 1, buf, bufSize);
      assertEquals(kv.valueSize, size);
      if (size > 0) {
        assertEquals(0, Utils.compareTo(buf, size, kv.valuePtr, kv.valueSize));
      }
    }
    UnsafeAccess.free(buf);
    UnsafeAccess.free(valuePtr);
  }

  @Test
  public void testBitCount() {
    log.debug(getTestParameters());

    int valueSize = 1000;
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    Utils.fillRandom(valuePtr, valueSize);
    KeyValue kv = keyValues.get(0);
    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    assertEquals(valueSize, size);

    int expCount = bitcount(valuePtr, valueSize, Commons.NULL_LONG, Commons.NULL_LONG);
    int count =
        (int) Strings.BITCOUNT(map, kv.keyPtr, kv.keySize, Commons.NULL_LONG, Commons.NULL_LONG);

    assertEquals(expCount, count);

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    // 1. start =-inf, end in range
    for (int i = 0; i < 100; i++) {
      long start = Commons.NULL_LONG;
      long end = r.nextInt(valueSize);
      expCount = bitcount(valuePtr, valueSize, start, end);
      count = (int) Strings.BITCOUNT(map, kv.keyPtr, kv.keySize, start, end);
      assertEquals(expCount, count);
    }

    // 2. start in range, end is +inf
    for (int i = 0; i < 100; i++) {
      long end = Commons.NULL_LONG;
      long start = r.nextInt(valueSize);
      expCount = bitcount(valuePtr, valueSize, start, end);
      count = (int) Strings.BITCOUNT(map, kv.keyPtr, kv.keySize, start, end);
      assertEquals(expCount, count);
    }

    // 2. start in range, end is too
    for (int i = 0; i < 200; i++) {
      long end = r.nextInt(valueSize);
      long start = r.nextInt(valueSize);
      expCount = bitcount(valuePtr, valueSize, start, end);
      count = (int) Strings.BITCOUNT(map, kv.keyPtr, kv.keySize, start, end);
      assertEquals(expCount, count);
    }

    UnsafeAccess.free(valuePtr);
  }

  private int bitcount(long ptr, int size, long start, long end) {
    if (start == Commons.NULL_LONG) {
      start = 0;
    }
    if (start < 0) {
      start = Math.max(start + size, 0);
    }
    if (end == Commons.NULL_LONG) {
      end = size - 1;
    }
    if (end < 0) {
      end += size;
    }
    if (end < 0 || start > end || start >= size) return 0;

    if (end >= size) {
      end = size - 1;
    }

    return (int) Utils.bitcount(ptr + start, (int) (end - start + 1));
  }

  @Test
  public void testBitPosition() {
    log.debug(getTestParameters());

    int valueSize = 1000;
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    Utils.fillRandom(valuePtr, valueSize);
    KeyValue kv = keyValues.get(0);
    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    assertEquals(valueSize, size);

    int expPos = (int) Utils.bitposSet(valuePtr, valueSize);
    int pos =
        (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 1, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expPos, pos);

    expPos = (int) Utils.bitposUnset(valuePtr, valueSize);
    pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 0, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expPos, pos);

    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    log.debug("Test seed={}", seed);

    // 1. start =-inf, end in range
    for (int i = 0; i < 100; i++) {
      long start = Commons.NULL_LONG;
      long end = r.nextInt(valueSize);
      expPos = bitpos(valuePtr, valueSize, 1, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 1, start, end);
      assertEquals(expPos, pos);
      expPos = bitpos(valuePtr, valueSize, 0, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 0, start, end);
      assertEquals(expPos, pos);
    }

    // 2. start in range, end is +inf
    for (int i = 0; i < 100; i++) {
      long end = Commons.NULL_LONG;
      long start = r.nextInt(valueSize);
      expPos = bitpos(valuePtr, valueSize, 1, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 1, start, end);
      assertEquals(expPos, pos);
      expPos = bitpos(valuePtr, valueSize, 0, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 0, start, end);
      assertEquals(expPos, pos);
    }

    // 2. start in range, end is too
    for (int i = 0; i < 200; i++) {
      long start = r.nextInt(valueSize);
      long end = r.nextInt(valueSize);
      expPos = bitpos(valuePtr, valueSize, 1, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 1, start, end);
      assertEquals(expPos, pos);
      expPos = bitpos(valuePtr, valueSize, 0, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 0, start, end);
      assertEquals(expPos, pos);
    }

    UnsafeAccess.free(valuePtr);
  }

  private int bitpos(long ptr, int size, int bit, long start, long end) {

    boolean startEndSet = start != Commons.NULL_LONG || end != Commons.NULL_LONG;

    if (start == Commons.NULL_LONG) {
      start = 0;
    }
    if (start < 0) {
      start = Math.max(start + size, 0);
    }
    if (end == Commons.NULL_LONG) {
      end = size - 1;
    }
    if (end < 0) {
      end += size;
    }
    if (end < 0 || start > end || start >= size) return -1;

    if (end >= size) {
      end = size - 1;
    }
    int pos = -1;
    if (bit == 1) {
      pos = (int) Utils.bitposSet(ptr + start, (int) (end - start + 1));
    } else if (bit == 0) {
      pos = (int) Utils.bitposUnset(ptr + start, (int) (end - start + 1));
    }
    if (pos >= 0) {
      pos += start * Utils.BITS_PER_BYTE;
    }
    if (pos == -1 && bit == 0 && !startEndSet) {
      pos = size * Utils.BITS_PER_BYTE;
    }
    return pos;
  }

  @Override
  public void extTearDown() {
    if (Objects.nonNull(keyValues)) {
      for (KeyValue k : keyValues) {
        UnsafeAccess.free(k.keyPtr);
        UnsafeAccess.free(k.valuePtr);
      }
    }
    UnsafeAccess.free(buffer);
  }
}

class FakeUserSession {

  static final String[] ATTRIBUTES =
      new String[] {
        "attr1", "attr2", "attr3", "attr4", "attr5",
        "attr6", "attr7", "attr8", "attr9", "attr10"
      };

  Properties props;

  FakeUserSession(Properties p) {
    this.props = p;
  }

  static FakeUserSession newSession(int i) {
    Properties p = new Properties();
    for (String attr : ATTRIBUTES) {
      p.put(attr, attr + ":value:" + i);
    }
    return new FakeUserSession(p);
  }

  public String toString() {
    return props.toString();
  }
}
