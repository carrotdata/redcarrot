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
package org.bigbase.carrot.util;

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

        Range() {
        }

        Range(long start, int size) {
            this.start = start;
            this.size = size;
        }

        @Override
        public int compareTo(Range o) {
            return Long.compare(start, o.start);
        }
    }

    private final TreeMap<Range, Range> treeMap = new TreeMap<>();

    public RangeTree() {
    }

    public synchronized Range add(Range r) {
        return treeMap.put(r, r);
    }

    public synchronized Range delete(long address) {
        search.start = address;
        search.size = 0;
        return treeMap.remove(search);
    }

    private final Range search = new Range();

    public synchronized boolean inside(long startKey, int sizeKey) {
        search.start = startKey;
        search.size = sizeKey;
        Range rangeValue = treeMap.floorKey(search);
//        boolean result = !Objects.isNull(rangeValue) && startKey >= rangeValue.start && startKey + sizeKey <= rangeValue.start + rangeValue.size;
        boolean result = !Objects.isNull(rangeValue) && rangeValue.start + rangeValue.size >= rangeValue.start + sizeKey;

        log.debug("floorKey start key {}, size key: {}, treeMap.size: {}", startKey, sizeKey, treeMap.size());
        if (!result) {
            if (!Objects.isNull(rangeValue)) {
                log.debug(
                        "Check FAILED for key [{},{}] range value allocation [{},{}] diff [{}, {}]",
                        startKey, sizeKey, rangeValue.start, rangeValue.size, startKey - rangeValue.start, sizeKey - rangeValue.size);
            } else {
                log.debug("Check FAILED for key [{},{}] No value allocation found.", startKey, sizeKey);
            }
        }
        return result;
    }

    public synchronized int size() {
        return treeMap.size();
    }

    public synchronized Set<Map.Entry<Range, Range>> entrySet() {
        return treeMap.entrySet();
    }
}
