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
package org.bigbase.carrot.examples.twitter;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Assert;
import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapScanner;
import org.bigbase.carrot.IndexBlock;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;

/**
 * Redis Book. Simple social network application. See description here:
 * https://redislabs.com/ebook/part-2-core-concepts/chapter-8-building-a-simple-social-network/
 *
 * <p>We implemented the following data types from the above book: user, user statuses, timelines,
 * followers, following in Carrot and Redis and compared memory usage per average user.
 *
 * <p>Some assumptions have been made, based on available information on the Internet:
 *
 * <p>1. We tried to use followers distribution which is as close to the real (Twitter) as possible.
 * The majority of users have pretty small number of followers (median is less than 100) but some
 * influential can have 100's of thousands and even millions. 2. Number of following is pretty small
 * and does not vary as much as the number of followers 3. On average, user makes 2 posts (status
 * updates) per day. 4. User registration date is a random date between 2010 and now. So If user
 * registered in 2010 he (she) made 2 * 365 * 10 = 7300 status updates.
 *
 * <p>Every User has the following data sets:
 *
 * <p>User - user's personal data User status - list of all messages posted by user (without order)
 * Profile Timeline - all messages posted by a user in a reverse chronological order. Followers -
 * list of user's followers Following - list of other user's this user is following to.
 *
 * <p>We measure the overall memory usage per one average social network user.
 *
 * <p>CARROT RESULTS:
 *
 * <p>-- User average memory size (bytes):
 *
 * <p>No compression - 151 LZ4 compression - 89 LZ4HC compression - 88
 *
 * <p>-- User statuses average memory size (bytes):
 *
 * <p>No compression - 456470 LZ4 compression - 211431 LZ4HC compression - 204365
 *
 * <p>-- User Timeline average memory size (bytes):
 *
 * <p>No compression - 76780 LZ4 compression - 57075 LZ4HC compression - 53794
 *
 * <p>-- User Followers average memory size (bytes):
 *
 * <p>No compression - 18,620 LZ4 compression - 15,362 LZ4HC compression - 15,432
 *
 * <p>-- User Following average memory size (bytes):
 *
 * <p>No compression - 29,760 LZ4 compression - 26,576 LZ4HC compression - 26,528
 *
 * <p>TOTAL size (user, user statuses, profile timeline, followers, following):
 *
 * <p>No compression ~ 580,000 LZ4 compression ~ 311,000 LZ4HC compression ~ 301,000
 *
 * <p>REDIS RESULTS:
 *
 * <p>-- User average memory size (bytes): 219 -- User statuses average memory size (bytes): 718,183
 * -- User Timeline average memory size (bytes): 420,739 -- User Followers average memory size
 * (bytes): 33,700 -- User Following average memory size (bytes): 56,790
 *
 * <p>TOTAL size (user, user statuses, profile timeline, followers, following): 1,229,631
 *
 * <p>OVERALL RESULTS:
 *
 * <p>Redis 6.0.10 ~ 1,300,000 Carrot (no compression) ~ 580,000 Carrot (LZ4) ~ 311,000 Carrot
 * (LZ4HC) ~ 301,000
 *
 * <p>Redis - to -Carrot RAM usage:
 *
 * <p>Redis/ Carrot no compression = 2.24 Redis/ Carrot LZ4 compression = 4.18 Redis/ Carrot LZ4HC
 * compression = 4.32
 */
public class TestCarrotTwitter {

  private static final Logger log = LogManager.getLogger(TestCarrotTwitter.class);

  static {
    // UnsafeAccess.debug = true;
  }

  static double avg_user_size;
  static double avg_user_status_size;
  static double avg_user_timeline_size;
  static double avg_user_followers_size;
  static double avg_user_following_size;

  private static void printSummary() {
    log.debug(
        "Carrot memory usage per user (user, statuses, profile timeline, followers, following)={}",
        avg_user_size
            + avg_user_status_size
            + avg_user_timeline_size
            + avg_user_followers_size
            + avg_user_following_size);
  }

  public static void main(String[] args) {
    runUsersNoCompression();
    runUsersLZ4Compression();
    runUsersLZ4HCCompression();
    runUserStatusNoCompression();
    runUserStatusLZ4Compression();
    runUserStatusLZ4HCCompression();

    runUserTimelinesNoCompression();
    runUserTimelineLZ4Compression();
    runUserTimelineLZ4HCCompression();

    runUserFollowersNoCompression();
    runUserFollowersLZ4Compression();
    runUserFollowersLZ4HCCompression();
    runUserFollowingNoCompression();
    runUserFollowingLZ4Compression();
    runUserFollowingLZ4HCCompression();
    printSummary();
  }

  private static void runUsers() {
    int numUsers = 100000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for (User u : users) {
      count++;
      u.saveToCarrot(map);
      if (count % 10000 == 0) {
        log.debug("Loaded {} users", count);
      }
    }

    count = (int) countRecords(map);

    if (count != numUsers) {
      log.fatal("count={} expected={}", count, numUsers);
      System.exit(-1);
    }
    count = 0;
    IndexBlock.DEBUG = true;

    for (User u : users) {
      if (!u.verify(map)) {
        log.fatal("Can't verify map");
        System.exit(-1);
      }
      if (++count % 10000 == 0) {
        log.debug("Verified {} users", count);
      }
    }
    long memory = BigSortedMap.getGlobalAllocatedMemory();
    avg_user_size = (double) memory / numUsers;
    map.dispose();
    log.debug("avg_user_size={} bytes", avg_user_size);
  }

  private static long countRecords(BigSortedMap map) {
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long count = 0;
    try {
      while (scanner.hasNext()) {
        count++;
        scanner.next();
      }
    } catch (IOException e) {
      return -1;
    } finally {
      try {
        scanner.close();
      } catch (IOException e) {
      }
    }
    return count;
  }

  private static void runUsersNoCompression() {
    log.debug("\nTest Users, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUsers();
  }

  private static void runUsersLZ4Compression() {
    log.debug("\nTest Users, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUsers();
  }

  private static void runUsersLZ4HCCompression() {
    log.debug("\nTest Users, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUsers();
  }

  private static void runUserStatuses() {
    int numUsers = 1000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    List<UserStatus> statuses = null;
    int count = 0;
    for (User user : users) {
      count++;
      statuses = UserStatus.newUserStatuses(user);
      for (UserStatus us : statuses) {
        us.saveToCarrot(map);
      }
      if (count % 100 == 0) {
        log.debug("Loaded {} user statuses", count);
      }
    }
    count = 0;
    for (UserStatus u : statuses) {
      if (!u.verify(map)) {
        log.debug("Can't verify map");
        System.exit(-1);
      }
      if (++count % 10000 == 0) {
        log.debug("Verified {} users", count);
      }
    }
    long memory = BigSortedMap.getGlobalAllocatedMemory();
    avg_user_status_size = (double) memory / numUsers;
    map.dispose();
    log.debug("avg_user_status_size={} bytes", avg_user_status_size);
  }

  private static void runUserStatusNoCompression() {
    log.debug("\nTest User Status, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUserStatuses();
  }

  private static void runUserStatusLZ4Compression() {
    log.debug("\nTest User Status, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUserStatuses();
  }

  private static void runUserStatusLZ4HCCompression() {
    log.debug("\nTest User Status, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUserStatuses();
  }

  private static void runUserTimelines() {
    int numUsers = 1000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for (User user : users) {
      count++;
      Timeline timeline = new Timeline(user);
      timeline.saveToCarrot(map);
      if (count % 100 == 0) {
        log.debug("Loaded {} user timelines", count);
      }
    }

    long memory = BigSortedMap.getGlobalAllocatedMemory();
    avg_user_timeline_size = (double) memory / numUsers;
    map.dispose();
    log.debug("avg_user_timeline_size={} bytes", avg_user_timeline_size);
  }

  private static void runUserTimelinesNoCompression() {
    log.debug("\nTest User Timeline, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUserTimelines();
  }

  private static void runUserTimelineLZ4Compression() {
    log.debug("\nTest User Timeline, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUserTimelines();
  }

  private static void runUserTimelineLZ4HCCompression() {
    log.debug("\nTest User Timeline, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUserTimelines();
  }

  private static void runUserFollowers() {
    int numUsers = 10000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    long total = 0;
    for (User user : users) {
      count++;
      Followers followers = new Followers(user);
      total += followers.size();
      followers.saveToCarrot(map);
      if (count % 100 == 0) {
        log.debug("Loaded {} user followers", count);
      }
    }

    long memory = BigSortedMap.getGlobalAllocatedMemory();
    avg_user_followers_size = (double) memory / numUsers;
    map.dispose();
    log.debug(
        "avg_user_followers_size={} bytes. Avg #folowers={}",
        avg_user_followers_size,
        total / numUsers);
  }

  private static void runUserFollowersNoCompression() {
    log.debug("\nTest User Followers, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUserFollowers();
  }

  private static void runUserFollowersLZ4Compression() {
    log.debug("\nTest User Followers, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUserFollowers();
  }

  private static void runUserFollowersLZ4HCCompression() {
    log.debug("\nTest User Followers, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUserFollowers();
  }

  private static void runUserFollowing() {
    int numUsers = 10000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    long total = 0;
    for (User user : users) {
      count++;
      Following following = new Following(user);
      total += following.size();
      following.saveToCarrot(map);
      if (count % 1000 == 0) {
        log.debug("Loaded {}  user following", count);
      }
    }

    long memory = BigSortedMap.getGlobalAllocatedMemory();
    avg_user_following_size = (double) memory / numUsers;
    map.dispose();
    log.debug(
        "avg_user_following_size={} bytes. Avg #folowing={}",
        avg_user_following_size,
        total / numUsers);
  }

  private static void runUserFollowingNoCompression() {
    log.debug("\nTest User Following, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUserFollowing();
  }

  private static void runUserFollowingLZ4Compression() {
    log.debug("\nTest User Following, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUserFollowing();
  }

  private static void runUserFollowingLZ4HCCompression() {
    log.debug("\nTest User Following, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUserFollowing();
  }
}
