/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis.hashes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.CarrotCoreBase;
import com.carrotdata.redcarrot.ops.OperationFailedException;
import com.carrotdata.redcarrot.util.Pair;
import com.carrotdata.redcarrot.util.Utils;
import org.junit.Before;
import org.junit.Test;

public class HashesAPITest extends CarrotCoreBase {

  private static final Logger log = LogManager.getLogger(HashesAPITest.class);

  private int nSize;

  public HashesAPITest(Object c) {
    super(c);
  }

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
    nSize = memoryDebug ? 100 : 1000;
  }

  @Override
  public void extTearDown() {
  }

  private List<String> loadData(String key, int n) {
    List<String> list = new ArrayList<>();
    Random r = new Random();
    for (int i = 0; i < n; i++) {
      String m = Utils.getRandomStr(r, 10);
      list.add(m);
      int res = Hashes.HSET(map, key, m, m);
      assertEquals(1, res);
      if (i % (nSize / 10) == 0) {
        log.debug("Loaded {}", i);
      }
    }
    return list;
  }

  private List<String> loadDataRandomSize(String key, int n) {
    List<String> list = new ArrayList<>();
    Random r = new Random();
    for (int i = 0; i < n; i++) {
      int size = r.nextInt(10) + 10;
      String m = Utils.getRandomStr(r, size);
      list.add(m);
      int res = Hashes.HSET(map, key, m, m);
      assertEquals(1, res);
      if (i % (nSize / 10) == 0) {
        log.debug("Loaded {}", i);
      }
    }
    return list;
  }

  @Test
  public void testHashExists() {

    int X = nSize;
    String key = "key";
    List<String> list = loadDataRandomSize(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    Random r = new Random();
    for (String f : list) {
      int res = Hashes.HEXISTS(map, key, f);
      assertEquals(1, res);
    }
    // Check non-existing fields
    for (int i = 0; i < nSize; i++) {
      String v = Utils.getRandomStr(r, 10);
      int res = Hashes.HEXISTS(map, key, v);
      assertEquals(0, res);
    }
    // Check other keys
    for (String f : list) {
      String k = Utils.getRandomStr(r, 10);
      int res = Hashes.HEXISTS(map, k, f);
      assertEquals(0, res);
    }
  }

  @Test
  public void testHashGet() {

    int X = nSize;
    String key = "key";
    List<String> list = loadDataRandomSize(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    Random r = new Random();
    for (String f : list) {
      String res = Hashes.HGET(map, key, f, 200);
      assertEquals(f, res);
    }
    // Check non-existing fields
    for (int i = 0; i < nSize; i++) {
      String v = Utils.getRandomStr(r, 10);
      String res = Hashes.HGET(map, key, v, 200);
      assertNull(res);
    }
    // Check other keys
    for (String f : list) {
      String k = Utils.getRandomStr(r, 10);
      String res = Hashes.HGET(map, k, f, 200);
      assertNull(res);
    }

    // Check small buffer
    for (String f : list) {
      String res = Hashes.HGET(map, key, f, 4);
      assertNull(res);
    }
  }

  @Test
  public void testHashIncrementFloat() throws OperationFailedException {

    int N = nSize;
    List<String> keys = new ArrayList<>();
    String field = "field";
    Random r = new Random();
    for (int i = 0; i < N; i++) {
      String key = Utils.getRandomStr(r, 10);
      keys.add(key);
      double value = Hashes.HINCRBYFLOAT(map, key, field, 1);
      assertEquals(1d, value, 0.0);
    }

    for (String key : keys) {
      double value = Hashes.HINCRBYFLOAT(map, key, field, 10);
      assertEquals(11d, value, 0.0);
      value = Hashes.HINCRBYFLOAT(map, key, field, 100);
      assertEquals(111d, value, 0.0);
      value = Hashes.HINCRBYFLOAT(map, key, field, 0);
      assertEquals(111d, value, 0.0);
      value = Hashes.HINCRBYFLOAT(map, key, field, -100);
      assertEquals(11d, value, 0.0);
      value = Hashes.HDEL(map, key, field);
      assertEquals(1d, value, 0.0);
    }
  }

  @Test
  public void testHashIncrement() throws OperationFailedException {

    int N = nSize;
    List<String> keys = new ArrayList<>();
    String field = "field";
    Random r = new Random();
    for (int i = 0; i < N; i++) {
      String key = Utils.getRandomStr(r, 10);
      keys.add(key);
      long value = Hashes.HINCRBY(map, key, field, 1);
      assertEquals(1L, value);
    }

    for (String key : keys) {
      long value = Hashes.HINCRBY(map, key, field, 10);
      assertEquals(11L, value);
      value = Hashes.HINCRBY(map, key, field, 100);
      assertEquals(111L, value);
      value = Hashes.HINCRBY(map, key, field, 0);
      assertEquals(111L, value);
      value = Hashes.HINCRBY(map, key, field, -100);
      assertEquals(11L, value);
      value = Hashes.HDEL(map, key, field);
      assertEquals(1L, value);
    }
  }

  @Test
  public void testHashDelete() {

    int X = nSize;
    String key = "key";
    List<String> list = loadDataRandomSize(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    Random r = new Random();
    // Delete - return previous value
    for (String f : list) {
      String res = Hashes.HDEL(map, key, f, 200);
      assertEquals(f, res);
    }
    // Check do not exists
    for (String f : list) {
      int res = Hashes.HEXISTS(map, key, f);
      assertEquals(0, res);
    }

    list = loadDataRandomSize(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    // Delete
    for (String f : list) {
      int res = Hashes.HDEL(map, key, f);
      assertEquals(1, res);
    }

    // Check do not exists
    for (String f : list) {
      int res = Hashes.HEXISTS(map, key, f);
      assertEquals(0, res);
    }

    list = loadDataRandomSize(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));

    // Check non-existing fields
    for (int i = 0; i < nSize; i++) {
      String v = Utils.getRandomStr(r, 10);
      String res = Hashes.HDEL(map, key, v, 200);
      assertNull(res);
    }

    // Check other keys
    for (String f : list) {
      String k = Utils.getRandomStr(r, 10);
      String res = Hashes.HDEL(map, k, f, 200);
      assertNull(res);
    }

    // Check small buffer
    for (String f : list) {
      String res = Hashes.HDEL(map, key, f, 4);
      assertNull(res);
    }
  }

  @Test
  public void testHashSetNonExistent() {

    int X = nSize;
    String key = "key";
    List<String> list = loadDataRandomSize(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    Random r = new Random();

    // Check existing fields
    for (int i = 0; i < nSize; i++) {
      int index = r.nextInt(list.size());
      String v = list.get(index);
      int res = Hashes.HSETNX(map, key, v, v);
      assertEquals(0, res);
    }
    // Check non-existing fields
    for (int i = 0; i < nSize; i++) {
      String v = Utils.getRandomStr(r, 10);
      int res = Hashes.HSETNX(map, key, v, v);
      assertEquals(1, res);
    }
  }

  @Test
  public void testHashValueLength() {

    int X = nSize;
    String key = "key";
    List<String> list = loadDataRandomSize(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    Random r = new Random();

    // Check existing fields
    for (int i = 0; i < nSize; i++) {
      int index = r.nextInt(list.size());
      String v = list.get(index);
      int size = Hashes.HSTRLEN(map, key, v);
      assertEquals(v.length(), size);
    }
    // Check non-existing fields
    for (int i = 0; i < nSize; i++) {
      int index = r.nextInt(list.size());
      String v = list.get(index);
      v += "111111111111111";
      int size = Hashes.HSTRLEN(map, key, v);
      assertEquals(0, size);
    }
  }

  @Test
  public void testHashMultiGet() {

    int X = nSize;
    String key = "key";
    List<String> list = loadData(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    int m = 10;
    // Run calls with correct buffer size
    int bufSize = m * (10 + 4) + 5;
    for (int i = 0; i < 100; i++) {
      String[] fields = getRandom(list, m);
      List<String> result = Hashes.HMGET(map, key, fields, bufSize);
      assertEquals(m, result.size());
      for (int k = 0; k < m; k++) {
        assertEquals(fields[k], result.get(k));
      }
    }

    // Run call with a small buffer
    String[] fields = getRandom(list, m + 1);
    List<String> result = Hashes.HMGET(map, key, fields, bufSize);
    assertEquals(0, result.size());

    // Check nulls
    fields = new String[] { list.get(0), "absd", list.get(2), "123", "679" };
    result = Hashes.HMGET(map, key, fields, bufSize);

    assertEquals(fields.length, result.size());

    assertEquals(list.get(0), result.get(0));
    assertNull(result.get(1));
    assertEquals(list.get(2), result.get(2));
    assertNull(result.get(3));
    assertNull(result.get(4));
  }

  private String[] getRandom(List<String> list, int count) {
    long[] arr = Utils.randomDistinctArray(list.size(), count);
    String[] ss = new String[count];
    for (int i = 0; i < arr.length; i++) {
      ss[i] = list.get((int) arr[i]);
    }
    return ss;
  }

  @Test
  public void testHashGetAllAPI() {

    // Load X elements
    int X = nSize;
    String key = "key";
    List<String> list = loadData(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    // Call with a large buffer
    List<Pair<String>> fieldValues = Hashes.HGETALL(map, key, 22 * nSize + 5);
    assertEquals(list.size(), fieldValues.size());
    for (int i = 0; i < list.size(); i++) {
      String s = list.get(i);
      String ss = fieldValues.get(i).getFirst();
      assertEquals(s, ss);
    }
    // Call with a smaller buffer
    fieldValues = Hashes.HGETALL(map, key, 22 * nSize);
    assertEquals(0, fieldValues.size());
  }

  @Test
  public void testHashKeysAPI() {

    // Load X elements
    int X = nSize;
    String key = "key";
    List<String> list = loadData(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    // Call with a large buffer
    List<String> keys = Hashes.HKEYS(map, key, 11 * nSize + 5);
    assertEquals(list.size(), keys.size());
    assertTrue(list.containsAll(keys));
    // Call with a smaller buffer
    keys = Hashes.HKEYS(map, key, 11 * nSize);
    assertEquals(0, keys.size());
  }

  @Test
  public void testHashValuesAPI() {
    // Load X elements
    int X = nSize;
    String key = "key";
    List<String> list = loadData(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));
    // Call with a large buffer
    List<String> values = Hashes.HVALS(map, key, 11 * nSize + 5);
    assertEquals(list.size(), values.size());
    assertTrue(list.containsAll(values));
    // Call with a smaller buffer
    values = Hashes.HVALS(map, key, 11 * nSize);
    assertEquals(0, values.size());
    assertTrue(list.containsAll(values));
  }

  @Test
  public void testSscanNoRegex() {

    // Load X elements
    int X = nSize;
    String key = "key";
    Random r = new Random();
    List<String> list = loadData(key, X);

    Collections.sort(list);

    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));

    // Check full scan
    int count = 11; // required buffer size 22 * 11 + 4 = 246
    log.debug("scan: lastSeenMember = null");
    int total = scan(key, null, count, 250, null);
    assertEquals(X, total);

    // Check correctness of partial scans

    for (int i = 0; i < nSize; i++) {
      int index = r.nextInt(list.size());
      String lastSeen = list.get(index);
      int expected = list.size() - index - 1;
      total = scan(key, lastSeen, count, 250, null);
      assertEquals(expected, total);
    }

    // Check edge cases

    String before = "A";
    String after = "zzzzzzzzzzzzzzzz";

    total = scan(key, before, count, 250, null);
    assertEquals(X, total);
    total = scan(key, after, count, 250, null);
    assertEquals(0, total);

    // Test buffer underflow - small buffer
    // buffer size is less than needed to keep 'count' members

    total = scan(key, before, count, 100, null);
    assertEquals(X, total);
    total = scan(key, after, count, 100, null);
    assertEquals(0, total);
  }

  private int countMatches(List<String> list, int startIndex, String regex) {
    int total = 0;
    List<String> subList = list.subList(startIndex, list.size());
    for (String s : subList) {
      if (s.matches(regex)) {
        total++;
      }
    }
    return total;
  }

  @Test
  public void testSscanWithRegex() {

    // Load X elements
    int X = nSize;
    String key = "key";
    String regex = "^A.*";
    Random r = new Random();
    List<String> list = loadData(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int) Hashes.HLEN(map, key));

    // Check full scan
    int expected = countMatches(list, 0, regex);
    int count = 11;
    int total = scan(key, null, count, 250, regex);
    assertEquals(expected, total);

    // Check correctness of partial scans

    for (int i = 0; i < 100; i++) {
      int index = r.nextInt(list.size());
      String lastSeen = list.get(index);
      String pattern = "^" + lastSeen.charAt(0) + ".*";
      expected = index == list.size() - 1 ? 0 : countMatches(list, index + 1, pattern);
      total = scan(key, lastSeen, count, 250, pattern);
      assertEquals(expected, total);
    }

    // Check edge cases

    String before = "A"; // less than any values
    String after = "zzzzzzzzzzzzzzzz"; // larger than any values
    expected = countMatches(list, 0, regex);

    total = scan(key, before, count, 250, regex);
    assertEquals(expected, total);
    total = scan(key, after, count, 250, regex);
    assertEquals(0, total);

    // Test buffer underflow - small buffer
    // buffer size is less than needed to keep 'count' members
    expected = countMatches(list, 0, regex);
    total = scan(key, before, count, 100, regex);
    assertEquals(expected, total);
    total = scan(key, after, count, 100, regex);
    assertEquals(0, total);
  }

  private int scan(String key, String lastSeenMember, int count, int bufferSize, String regex) {
    int total = 0;
    List<Pair<String>> result;
    // Check overall functionality - full scan
    while ((result = Hashes.HSCAN(map, key, lastSeenMember, count, bufferSize, regex)) != null) {
      total += result.size() - 1;
      lastSeenMember = result.get(result.size() - 1).getFirst();
    }
    return total;
  }

  @Test
  public void testScannerRandomMembersEdgeCases() {

    // Load N elements
    int N = nSize;
    String key = "key";
    List<String> list = loadData(key, N);

    Collections.sort(list);

    List<Pair<String>> result = Hashes.HRANDFIELD(map, key, 100, false, 1105); // Required size is
                                                                               // 1104 for 100
                                                                               // elements
    assertEquals(100, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, false, 1104); // Required size is 1104 for 100
                                                            // elements
    assertEquals(100, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, false, 1103); // Required size is 1093 for 99 elements
    assertEquals(99, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, false, 1092); // Required size is 1082 for 98 elements
    assertEquals(98, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, false, 100); // Required size is 92 for 8 elements
    assertEquals(8, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, false, 15); // Required size is 15 for 1 element
    assertEquals(1, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, false, 10); // Required size is 15 for 1 element
    assertEquals(0, result.size());

    // Now with values
    result = Hashes.HRANDFIELD(map, key, 100, true, 2205); // Required size is 2204 for 100 elements
    assertEquals(100, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, true, 2204); // Required size is 2204 for 100 elements
    assertEquals(100, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, true, 2182); // Required size is 2182 for 99 elements
    assertEquals(99, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, true, 2160); // Required size is 2160 for 98 elements
    assertEquals(98, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, true, 180); // Required size is 180 for 8 elements
    assertEquals(8, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, true, 26); // Required size is 26 for 1 element
    assertEquals(1, result.size());

    result = Hashes.HRANDFIELD(map, key, 100, true, 10); // Required size is 26 for 1 element
    assertEquals(0, result.size());
  }

  @Test
  public void testScannerRandomMembers() {

    // Load N elements
    int N = nSize;
    int numIter = nSize;
    String key = "key";
    List<String> list = loadData(key, N);

    Collections.sort(list);

    long start = System.currentTimeMillis();
    for (int i = 0; i < numIter; i++) {
      List<Pair<String>> result = Hashes.HRANDFIELD(map, key, 10, false, 4096);
      assertEquals(10, result.size());
      assertTrue(unique(result));
      for (Pair<String> p : result) {
        assertTrue(list.contains(p.getFirst()));
      }
      if (i % 100 == 0) {
        log.debug("Skipped {}", i);
      }
    }
    long end = System.currentTimeMillis();
    log.debug("{} random members for {} cardinality set time={}ms", numIter, N, end - start);

    // Check negatives
    start = System.currentTimeMillis();
    for (int i = 0; i < numIter; i++) {
      List<Pair<String>> result = Hashes.HRANDFIELD(map, key, -10, false, 4096);
      assertEquals(10, result.size());
      for (Pair<String> p : result) {
        assertTrue(list.contains(p.getFirst()));
      }
      if (i % 100 == 0) {
        log.debug("Skipped {}", i);
      }
    }
    end = System.currentTimeMillis();
    log.debug(
      numIter + " random members for " + N + " cardinality set time=" + (end - start) + "ms");

    // Now check with values
    start = System.currentTimeMillis();
    for (int i = 0; i < numIter; i++) {
      List<Pair<String>> result = Hashes.HRANDFIELD(map, key, 10, true, 4096);
      assertEquals(10, result.size());
      assertTrue(unique(result));
      for (Pair<String> p : result) {
        assertTrue(list.contains(p.getFirst()));
      }
      if (i % 100 == 0) {
        log.debug("Skipped {}", i);
      }
    }
    end = System.currentTimeMillis();
    log.debug("{} random members for {} cardinality set time={}ms", numIter, N, end - start);

    // Check negatives
    start = System.currentTimeMillis();
    for (int i = 0; i < numIter; i++) {
      List<Pair<String>> result = Hashes.HRANDFIELD(map, key, -10, true, 4096);
      assertEquals(10, result.size());
      for (Pair<String> p : result) {
        assertTrue(list.contains(p.getFirst()));
        // *DEBUG*/log.debug(p.getFirst());
      }
      // log.debug();
      if (i % 100 == 0) {
        log.debug("Skipped {}", i);
      }
    }
    end = System.currentTimeMillis();
    log.debug("{} random members for {} cardinality set time={}ms", numIter, N, end - start);
  }

  /**
   * Checks if list has all unique members. List must be sorted.
   * @param list List of unique values
   * @return true/false
   */
  private boolean unique(List<Pair<String>> list) {
    if (list.size() <= 1) return true;

    for (int i = 1; i < list.size(); i++) {
      if (list.get(i - 1).equals(list.get(i))) {
        return false;
      }
    }
    return true;
  }
}
