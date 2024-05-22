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
package com.carrotdata.redcarrot.examples.appcomps;

import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

import com.carrotdata.redcarrot.examples.util.KeyValues;
import com.carrotdata.redcarrot.util.Bytes;
import com.carrotdata.redcarrot.util.Key;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

public class Stats extends KeyValues {

  static final String MIN = "min";
  static final String MAX = "max";
  static final String COUNT = "count";
  static final String SUM = "sum";
  static final String SUMSQ = "sumsq";

  long hourStartTime;

  protected Stats(Properties p) {
    super(p);
  }

  private static long getHourStartTime(int hoursBefore) {
    Calendar cal = Calendar.getInstance();
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH);
    int day = cal.get(Calendar.DAY_OF_MONTH);
    int hour = cal.get(Calendar.HOUR_OF_DAY);
    cal.set(year, month, day, hour, 0);
    long time0 = cal.getTimeInMillis();
    long time = time0 - hoursBefore * 3600 * 1000;
    return time;
  }

  public static Stats newStats(int hoursBefore) {
    Properties p = new Properties();
    Random r = new Random();
    p.setProperty(MIN, Integer.toString(r.nextInt(100)));
    p.setProperty(MAX, Integer.toString(r.nextInt(100) + 100));
    p.setProperty(COUNT, Long.toString(r.nextInt(10000)));
    p.setProperty(SUM, Long.toString(r.nextInt(1000000)));
    p.setProperty(SUMSQ, Long.toString(Math.abs(r.nextLong())));
    Stats st = new Stats(p);
    st.hourStartTime = getHourStartTime(hoursBefore);
    ;
    return st;
  }

  @Override
  public String getKey() {
    return "stats:profilepage:accesstime:";
  }

  @Override
  public Key getKeyNative() {
    String key = getKey();
    long ptr = UnsafeAccess.allocAndCopy(key, 0, key.length() + Utils.SIZEOF_LONG);
    UnsafeAccess.putLong(ptr + key.length(), Long.MAX_VALUE - hourStartTime);
    return new Key(ptr, key.length() + Utils.SIZEOF_LONG);
  }

  @Override
  public byte[] getKeyBytes() {
    return Bytes.add(getKey().getBytes(), Bytes.toBytes(Long.MAX_VALUE - hourStartTime));
  }
}
