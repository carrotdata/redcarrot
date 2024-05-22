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
package com.carrotdata.common.nativelib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Provides OS name and architecture name. */
public class OSInfo {
  private static final Logger log = LogManager.getLogger(OSInfo.class);

  public static void main(String[] args) {
    if (args.length >= 1) {
      if ("--os".equals(args[0])) {
        log.debug(getOSName());
        return;
      } else if ("--arch".equals(args[0])) {
        log.debug(getArchName());
        return;
      }
    }

    log.debug(getNativeLibFolderPathForCurrentOS());
  }

  public static String getNativeLibFolderPathForCurrentOS() {
    return getOSName() + "/" + getArchName();
  }

  public static String getOSName() {
    return translateOSNameToFolderName(System.getProperty("os.name"));
  }

  public static String getArchName() {
    return translateArchNameToFolderName(System.getProperty("os.arch"));
  }

  public static String translateOSNameToFolderName(String osName) {
    if (osName.contains("Windows")) {
      return "Windows";
    } else if (osName.contains("Mac")) {
      return "Mac";
    } else if (osName.contains("Linux")) {
      return "Linux";
    } else {
      return osName.replaceAll("\\W", "");
    }
  }

  public static String translateArchNameToFolderName(String archName) {
    return archName.replaceAll("\\W", "");
  }
}
