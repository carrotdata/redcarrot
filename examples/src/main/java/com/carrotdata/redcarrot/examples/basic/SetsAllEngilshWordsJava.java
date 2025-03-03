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
package com.carrotdata.redcarrot.examples.basic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SetsAllEngilshWordsJava {

  private static final Logger log = LogManager.getLogger(SetsAllEngilshWordsJava.class);

  
  static List<HashSet<String>> sets = new ArrayList<HashSet<String>>();


  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      usage();
    }
    runTest(args[0]);
  }

  private static void runTest(String fileName) throws IOException {

    List<String> lines = Files.readAllLines(Path.of(fileName));
    int loop = 0;
    long startTime = System.currentTimeMillis();
    try {
      while (true) {
        HashSet<String> set = new HashSet<String>();
        for (String e: lines) {
          set.add(new String(e));
        }
        sets.add(set);
        loop++;
        log.info(loop);
      }
    } catch (Throwable t) {
      log.error(t);
    }
    long endTime = System.currentTimeMillis();
    log.debug("Loaded {} sets, in {}ms", loop, endTime - startTime);

  }

  private static void usage() {
    log.fatal("usage: java com.carrotdata.redcarrot.examples.SetsAllEnglishWordsJava file");
    System.exit(-1);
  }
}
