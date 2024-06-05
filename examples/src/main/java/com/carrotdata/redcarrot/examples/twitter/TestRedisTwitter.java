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

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

public class TestRedisTwitter {

  private static final Logger log = LogManager.getLogger(TestRedisTwitter.class);

  static Jedis client = new Jedis("localhost");

  public static void main(String[] args) throws IOException {
    runUsers();
    runUserStatuses();
    runUserTimelines();
    runUserFollowers();
    runUserFollowing();
  }

  private static void runUsers() throws IOException {
    log.debug("Run Users");
    int numUsers = 100000;
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for (User u : users) {
      count++;
      u.saveToRedis(client);
      if (count % 10000 == 0) {
        log.debug("Loaded {} users", count);
      }
    }
    log.debug("Print any button ...");
    System.in.read();
  }

  private static void runUserStatuses() throws IOException {
    int numUsers = 1000;
    log.debug("Run User Statuses");

    List<User> users = User.newUsers(numUsers);
    List<UserStatus> statuses = null;
    int count = 0;
    for (User user : users) {
      count++;
      statuses = UserStatus.newUserStatuses(user);
      for (UserStatus us : statuses) {
        us.saveToRedis(client);
      }
      if (count % 100 == 0) {
        log.debug("Loaded {} user statuses", count);
      }
    }

    log.debug("Print any button ...");
    System.in.read();
  }

  private static void runUserTimelines() throws IOException {
    int numUsers = 1000;
    log.debug("Run User Timeline");

    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for (User user : users) {
      count++;
      Timeline timeline = new Timeline(user);
      timeline.saveToRedis(client);
      if (count % 100 == 0) {
        log.debug("Loaded {} user timelines", count);
      }
    }
    log.debug("Print any button ...");
    System.in.read();
  }

  private static void runUserFollowers() throws IOException {
    int numUsers = 10000;
    log.debug("Run User Followers");

    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for (User user : users) {
      count++;
      Followers followers = new Followers(user);
      followers.saveToRedis(client);
      if (count % 100 == 0) {
        log.debug("Loaded {} user followers", count);
      }
    }

    log.debug("Print any button ...");
    System.in.read();
  }

  private static void runUserFollowing() throws IOException {
    int numUsers = 10000;
    log.debug("Run User Following");

    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for (User user : users) {
      count++;
      Following following = new Following(user);
      following.saveToRedis(client);
      if (count % 1000 == 0) {
        log.debug("Loaded {} user following", count);
      }
    }

    log.debug("Print any button ...");
    System.in.read();
  }
}
