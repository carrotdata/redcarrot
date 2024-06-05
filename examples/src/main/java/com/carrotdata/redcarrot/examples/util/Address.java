/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.examples.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Address extends KeyValues {

  private static final Logger log = LogManager.getLogger(Address.class);

  Address(Properties p) {
    super(p);
  }

  static String[] ATTRIBUTES = new String[] { "LON", "LAT", "NUMBER", "STREET", "UNIT", "CITY",
      "DISTRICT", "REGION", "POSTCODE", "ID", "HASH" };

  @SuppressWarnings("deprecation")
  public static List<Address> loadFromFile(String name) throws IOException {
    // We filter ill-formatted and those which are missing

    FileInputStream fis = new FileInputStream(name);
    DataInputStream dis = new DataInputStream(fis);
    List<Address> list = new ArrayList<Address>();

    String line = null;
    long total = 0;
    long valid = 0;
    long totalSize = 0;
    while ((line = dis.readLine()) != null) {
      total++;
      String[] arr = line.split(",");
      if (arr.length != ATTRIBUTES.length) {
        continue;
      }
      if (arr[3].length() == 0 || arr[5].length() == 0 || arr[7].length() == 0
          || arr[8].length() == 0) {
        continue;
      }
      valid++;
      Properties p = new Properties();
      // Skip LON, LAT and last two
      for (int i = 2; i < arr.length - 2; i++) {
        if (arr[i].length() == 0) continue;
        p.put(ATTRIBUTES[i], arr[i]);
        totalSize += ATTRIBUTES[i].length() + arr[i].length();
      }
      list.add(new Address(p));
      if ((list.size() % 10000) == 0) {
        log.debug("Loaded {}", list.size());
      }
    }
    dis.close();
    log.debug("Parsed file: {}\nTotal records={}\nValid records={}\nTotalSize={}", name, total,
      valid, totalSize);
    return list;
  }

  public static String getUserId(int n) {
    return "address:user:" + n;
  }

  @Override
  public String getKey() {
    // TODO Auto-generated method stub
    return null;
  }
}
