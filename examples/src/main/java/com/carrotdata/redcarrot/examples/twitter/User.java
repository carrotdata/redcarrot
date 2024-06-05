/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.examples.twitter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.examples.util.KeyValues;
import com.carrotdata.redcarrot.util.KeyValue;
import com.carrotdata.redcarrot.util.Utils;

/**
 * Twitter user object (simple)
 * <p>
 * login id name followers following posts sign-up
 * <p>
 * Followers distribution (2013):
 * <p>
 * Percentile Followers 10 3 20 9 30 19 40 36 50 61 60 98 70 154 80 246 90 458 95 819 96 978 97
 * 1,211 98 1,675 99 2,991 99.9 24,964
 */
public class User extends KeyValues {

  private static final Logger log = LogManager.getLogger(User.class);

  public static final String LOGIN = "login";
  public static final String ID = "id";
  public static final String NAME = "name";
  public static final String FOLLOWERS = "followers";
  public static final String FOLLOWING = "following";
  public static final String POSTS = "posts";
  public static final String SIGNUP = "signup";

  static Random rnd = new Random();

  static {
    long seed = rnd.nextLong();
    rnd.setSeed(seed);
    log.debug("SEED={}", seed);
  }

  static int[] perc =
      new int[] { 0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 950, 960, 970, 980, 990, 999 };
  static int[] nums =
      new int[] { 0, 3, 9, 19, 36, 61, 98, 154, 246, 458, 819, 978, 1211, 1675, 2991, 24964 };

  User(Properties p) {
    super(p);
  }

  public String getKey() {
    return "user:" + getId();
  }

  String getLogin() {
    return props.getProperty(LOGIN);
  }

  String getId() {
    return props.getProperty(ID);
  }

  String getName() {
    return props.getProperty(NAME);
  }

  int getTotalFollowers() {
    return Integer.valueOf(props.getProperty(FOLLOWERS));
  }

  int getFollowing() {
    return Integer.valueOf(props.getProperty(FOLLOWING));
  }

  int getPosts() {
    return Integer.valueOf(props.getProperty(POSTS));
  }

  long getSignup() {
    return Long.valueOf(props.getProperty(SIGNUP));
  }

  @Override
  public List<KeyValue> asList() {
    // Special handling for numeric properties
    ArrayList<KeyValue> list = new ArrayList<KeyValue>();
    String key = LOGIN;
    String value = getLogin();
    list.add(KeyValues.fromKeyValue(key, value));

    key = NAME;
    value = getName();
    list.add(KeyValues.fromKeyValue(key, value));

    key = ID;
    value = getId();
    list.add(KeyValues.fromKeyAndNumericValue(key, value));

    key = SIGNUP;
    value = Long.toString(getSignup());
    list.add(KeyValues.fromKeyAndNumericValue(key, value));

    key = POSTS;
    value = Integer.toString(getPosts());
    list.add(KeyValues.fromKeyAndNumericValue(key, value));

    key = FOLLOWERS;
    value = Integer.toString(getTotalFollowers());
    list.add(KeyValues.fromKeyAndNumericValue(key, value));

    key = FOLLOWING;
    value = Integer.toString(getFollowing());
    list.add(KeyValues.fromKeyAndNumericValue(key, value));

    return list;
  }

  @Override
  public Map<byte[], byte[]> asMap() {
    Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
    String key = LOGIN;
    String value = getLogin();
    map.put(key.getBytes(), value.getBytes());

    key = NAME;
    value = getName();
    map.put(key.getBytes(), value.getBytes());

    key = ID;
    value = getId();
    map.put(key.getBytes(), Utils.numericStrToBytes(value));

    key = SIGNUP;
    value = Long.toString(getSignup());
    map.put(key.getBytes(), Utils.numericStrToBytes(value));

    key = POSTS;
    value = Integer.toString(getPosts());
    map.put(key.getBytes(), Utils.numericStrToBytes(value));

    key = FOLLOWERS;
    value = Integer.toString(getTotalFollowers());
    map.put(key.getBytes(), Utils.numericStrToBytes(value));

    key = FOLLOWING;
    value = Integer.toString(getFollowing());
    map.put(key.getBytes(), Utils.numericStrToBytes(value));
    return map;
  }

  public static User newUser() {
    User u = null;
    Properties p = new Properties();
    p.setProperty(LOGIN, newLogin());
    String signup = newSignup();
    p.setProperty(SIGNUP, signup);
    long time = Long.valueOf(signup);
    p.setProperty(ID, newId(time));
    p.setProperty(NAME, newName());
    p.setProperty(FOLLOWERS, newFollowers());
    p.setProperty(FOLLOWING, newFollowing());
    p.setProperty(POSTS, newPosts(Long.parseLong(signup)));

    u = new User(p);
    return u;
  }

  public static List<User> newUsers(int n) {
    List<User> list = new ArrayList<User>();
    for (int i = 0; i < n; i++) {
      list.add(newUser());
    }
    return list;
  }

  private static String newLogin() {
    // random alphabetical string of size 8
    int min = 'a';
    int max = 'z';
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < 8; i++) {
      sb.append((char) (rnd.nextInt(max - min) + 'a'));
    }
    return sb.toString();
  }

  private static String newName() {
    // random alphabetical string of size 13
    // random alphabetical string of size 8
    int min = 'a';
    int max = 'z';
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < 13; i++) {
      sb.append((char) (rnd.nextInt(max - min) + 'a'));
    }
    return sb.toString();
  }

  private static String newId(long signup) {
    return Long.toString(Id.nextId(signup));
  }

  private static String newFollowers() {
    int n = rnd.nextInt(1000);
    int val = 0;
    int i = 0;
    for (; i < perc.length - 1; i++) {
      if (perc[i] <= n && perc[i + 1] >= n) {
        break;
      }
    }
    val = (int) ((double) ((perc[i + 1] - n) * nums[i] + (n - perc[i]) * nums[i + 1]))
        / (perc[i + 1] - perc[i]);
    return Integer.toString(val);
  }

  private static String newFollowing() {
    return Integer.toString(rnd.nextInt(1000));
  }

  /**
   * Posts twice a day
   * @return number of posts as a String
   */
  private static String newPosts(long signup) {
    long registered = Long.valueOf(signup);
    Calendar cal = Calendar.getInstance();
    Date today = cal.getTime();
    cal.setTimeInMillis(registered);
    Date regtime = cal.getTime();
    long diff = today.getTime() - regtime.getTime();
    return Long.toString(diff / (12 * 3600 * 1000));
  }

  private static String newSignup() {
    long period = (long) 10 * 365 * 24 * 3600 * 1000;
    long time = System.currentTimeMillis();
    return Long.toString(time - (long) (rnd.nextDouble() * period));
  }
}
