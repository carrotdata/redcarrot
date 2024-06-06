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

public class Pair<T> implements Comparable<Pair<T>> {

  T first;
  T second;

  public Pair(T first, T second) {
    this.first = first;
    this.second = second;
  }

  public T getFirst() {
    return first;
  }

  public T getSecond() {
    return second;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof Pair)) {
      return false;
    }
    Pair<T> p = (Pair<T>) o;
    return first.equals(p.first) && second.equals(p.second);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Pair<T> o) {
    // TODO Auto-generated method stub
    Comparable<T> t = (Comparable<T>) first;
    Comparable<T> s = (Comparable<T>) o.first;
    return t.compareTo((T) s);
  }
}
