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
package com.carrotdata.redcarrot.util;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RangeTree {

  private static final Logger log = LogManager.getLogger(RangeTree.class);

  public static class Range implements Comparable<Range> {
    long start;
    int size;

    // I add public to make a copy outside this class
    public Range() {}

    // I add public to make a copy outside this class
    public Range(long start, int size) {
      this.start = start;
      this.size = size;
    }

    public long getStart() {
      return start;
    }

    public int getSize() {
      return size;
    }

    @Override
    public int compareTo(Range o) {
      return Long.compare(start, o.start);
    }

    @Override
    public String toString() {
      return "Range{" + "start=" + start + ", size=" + size + '}';
    }
  }

  private final TreeMap<Range, Range> map = new TreeMap<>();

  public RangeTree() {}

  public synchronized Range add(Range r) {
    return map.put(r, r);
  }

  public synchronized Range delete(long address) {
    search.start = address;
    search.size = 0;
    return map.remove(search);
  }

  private final Range search = new Range();

  public synchronized boolean inside(long start, int size) {
    search.start = start;
    search.size = size;
    Range r = map.floorKey(search);
    boolean result = !Objects.isNull(r) && start >= r.start && start + size <= r.start + r.size;

    //    log.debug(
    //        "floorKey start key {}, size key: {}, range: {}",
    //        start,
    //        size,
    //        Objects.isNull(r) ? "null" : r.toString());
    if (!result) {
      if (!Objects.isNull(r)) {
        log.debug(
            "Check FAILED for range [{},{}] range allocation [{},{}] shift disposition [address: {}, size: {}]",
            start,
            size,
            r.start,
            r.size,
            r.start - start,
            r.size - size);
      } else {
        log.debug("Check FAILED for range [{},{}] No allocation found.", start, size);
      }
    }
    return result;
  }

  public synchronized int size() {
    return map.size();
  }

  public synchronized Set<Map.Entry<Range, Range>> entrySet() {
    return map.entrySet();
  }
}
