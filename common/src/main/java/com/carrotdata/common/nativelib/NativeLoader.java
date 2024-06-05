/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.common.nativelib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class NativeLoader {

  private static final Logger log = LogManager.getLogger(NativeLoader.class);

  private static HashMap<String, Boolean> loadedLibFiles = new HashMap<String, Boolean>();
  private static HashMap<String, Boolean> loadedLib = new HashMap<String, Boolean>();

  public static synchronized void load(String lib) {
    if (loadedLibFiles.containsKey(lib) && loadedLibFiles.get(lib) == true) return;

    try {
      System.load(lib);
      loadedLibFiles.put(lib, true);
    } catch (Exception e) {
      log.error("StackTrace: ", e);
      ;
    }
  }

  public static synchronized void loadLibrary(String libname) {
    if (loadedLib.containsKey(libname) && loadedLib.get(libname) == true) return;

    try {
      System.loadLibrary(libname);
      loadedLib.put(libname, true);
    } catch (Exception e) {
      log.error("StackTrace: ", e);
      ;
    }
  }
}
