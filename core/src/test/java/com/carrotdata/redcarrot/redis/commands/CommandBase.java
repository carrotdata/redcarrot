/*
 * Copyright (C) 2021-present Carrot, Inc. <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc. <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. <p>You should have received a copy
 * of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.redcarrot.redis.commands;

import static com.carrotdata.redcarrot.util.Utils.byteBufferToString;
import static com.carrotdata.redcarrot.util.Utils.strToByteBuffer;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.BigSortedMap;
import com.carrotdata.redcarrot.redis.CommandProcessor;
import com.carrotdata.redcarrot.redis.RedisConf;
import com.carrotdata.redcarrot.redis.RedisServer;
import com.carrotdata.redcarrot.redis.db.DBSystem;
import com.carrotdata.redcarrot.redis.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class CommandBase {

  private static final Logger log = LogManager.getLogger(CommandBase.class);

  protected static final String SKIP_VERIFY = "@"; // Skip verify if expected reply equals
  protected BigSortedMap map;
  protected ByteBuffer in, inDirect;
  protected ByteBuffer out, outDirect;

  /** Subclasses must override */
  protected abstract String[] getValidRequests();

  protected abstract String[] getValidResponses();

  protected abstract String[] getInvalidRequests();

  protected abstract String[] getInvalidResponses();

  @Before
  public void setUp() {
    map = new BigSortedMap(1000000);
    in = ByteBuffer.allocate(4096);
    out = ByteBuffer.allocate(4096);
    inDirect = ByteBuffer.allocateDirect(4096);
    outDirect = ByteBuffer.allocateDirect(4096);
  }

  @Test
  public void testValidRequests() {
    /* DEBUG */ log.debug("testValidRequests starts");
    String[] validRequests = getValidRequests();
    String[] validResponses = getValidResponses();

    for (int i = 0; i < validRequests.length; i++) {
      in.clear();
      out.clear();
      String inline = validRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      // *DEBUG*/ log.debug("VALID REQUEST {}", request);
      // *DEBUG*/ log.debug("EXP RESPONSE {}", validResponses[i]);

      strToByteBuffer(request, in);
      CommandProcessor.process(map, in, out);
      String result = byteBufferToString(out);
      // *DEBUG*/ log.debug("RESPONSE {}", result);

      if (validResponses[i].equals(SKIP_VERIFY)) {
        // TODO: we need some verification
      } else {
        assertEquals(validResponses[i], result);
      }
    }
    /* DEBUG */ log.debug("testValidRequests finishes");
  }

  private static boolean serverStarted = false;
  private static Socket client;
  private static DataOutputStream os;
  private static DataInputStream is;

  @Test
  public void testValidRequestsNetworkMode()
      throws UnknownHostException, IOException, InterruptedException {
    if (System.getProperty("surefire") != null) return;
    // Start server
    // Connect client
    /* DEBUG */ log.debug("testValidRequestsNetworkMode starts");

    if (!serverStarted) {
      RedisServer.start();
      // Connect client
      client = new Socket("localhost", RedisConf.getInstance().getServerPort());
      os = new DataOutputStream(client.getOutputStream());
      is = new DataInputStream(client.getInputStream());
      serverStarted = true;
    }

    String[] validRequests = addFlushallRequest(getValidRequests());
    String[] validResponses = addFlushallResponce(getValidResponses());

    for (int i = 0; i < validRequests.length; i++) {
      while (is.available() > 0) {
        is.read();
      }
      String inline = validRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      String expResponse = validResponses[i];
      byte[] expBytes = new byte[expResponse.length()];
      log.debug("REQUEST:");
      log.debug("{}", inline);

      os.write(request.getBytes());
      is.readFully(expBytes);

      String result = new String(expBytes);

      log.debug("\nRESULT:");
      log.debug(result);

      if (validResponses[i].equals(SKIP_VERIFY)) {
        // TODO: we need some verification
      } else {
        assertEquals(validResponses[i], result);
      }
    }
    /* DEBUG */ log.debug("testValidRequestsNetworkMode finishes");
  }

  private String[] addFlushallRequest(String[] requests) {
    String[] arr = new String[requests.length + 1];
    arr[0] = "flushall";
    System.arraycopy(requests, 0, arr, 1, requests.length);
    return arr;
  }

  private String[] addFlushallResponce(String[] resp) {
    String[] arr = new String[resp.length + 1];
    arr[0] = "+OK\r\n";
    System.arraycopy(resp, 0, arr, 1, resp.length);
    return arr;
  }

  @Test
  public void testValidRequestsInline() {
    /* DEBUG */ log.debug("testValidRequestsInline starts");

    String[] validRequests = getValidRequests();
    String[] validResponses = getValidResponses();
    for (int i = 0; i < validRequests.length; i++) {
      in.clear();
      out.clear();
      String inline = validRequests[i];
      String request = inline;

      strToByteBuffer(request, in);
      CommandProcessor.process(map, in, out);
      String result = byteBufferToString(out);
      log.info("INLINE REQUEST: {}", request);
      log.info("INLINE RESPONSE: {}", result);
      log.info("EXPECTED RESPONSE: {}", validResponses[i]);

      if (validResponses[i].equals(SKIP_VERIFY)) {
        log.debug(result);
      } else {
        assertEquals(validResponses[i], result);
      }
    }
    /* DEBUG */ log.debug("testValidRequestsInline finishes");
  }

  @Test
  public void testValidRequestsDirectBuffer() {
    /* DEBUG */ log.debug("testValidRequestsDirectBuffer starts");

    String[] validRequests = getValidRequests();
    String[] validResponses = getValidResponses();
    for (int i = 0; i < validRequests.length; i++) {
      inDirect.clear();
      outDirect.clear();
      String inline = validRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      strToByteBuffer(request, inDirect);
      CommandProcessor.process(map, inDirect, outDirect);
      String result = byteBufferToString(outDirect);
      if (validResponses[i].equals(SKIP_VERIFY)) {
        log.debug(result);
      } else {
        assertEquals(validResponses[i], result);
      }
    }
    /* DEBUG */ log.debug("testValidRequestsDirectBuffer finishes");
  }

  @Test
  public void testValidRequestsInlineDirectBuffer() {
    /* DEBUG */ log.debug("testValidRequestsInlineDirectBuffer starts");

    String[] validRequests = getValidRequests();
    String[] validResponses = getValidResponses();
    for (int i = 0; i < validRequests.length; i++) {
      inDirect.clear();
      outDirect.clear();
      String inline = validRequests[i];
      String request = inline;
      strToByteBuffer(request, inDirect);
      CommandProcessor.process(map, inDirect, outDirect);
      String result = byteBufferToString(outDirect);
      if (validResponses[i].equals(SKIP_VERIFY)) {
        log.debug(result);
      } else {
        assertEquals(validResponses[i], result);
      }
    }
    /* DEBUG */ log.debug("testValidRequestsInlineDirectBuffer finishes");
  }
  // INVALID REQUESTS

  @Test
  public void testInValidRequests() {
    String[] invalidRequests = getInvalidRequests();
    String[] invalidResponses = getInvalidResponses();
    for (int i = 0; i < invalidRequests.length; i++) {
      in.clear();
      out.clear();
      String inline = invalidRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      log.debug(inline);
      strToByteBuffer(request, in);
      CommandProcessor.process(map, in, out);
      String result = byteBufferToString(out);
      if (invalidResponses[i].equals(SKIP_VERIFY)) {
        log.debug(result);
      } else {
        assertEquals(invalidResponses[i], result);
      }
    }
  }

  @Test
  public void testInValidRequestsInline() {
    String[] invalidRequests = getInvalidRequests();
    String[] invalidResponses = getInvalidResponses();
    for (int i = 0; i < invalidRequests.length; i++) {
      in.clear();
      out.clear();
      String inline = invalidRequests[i];
      String request = inline;
      strToByteBuffer(request, in);
      CommandProcessor.process(map, in, out);
      String result = byteBufferToString(out);
      if (invalidResponses[i].equals(SKIP_VERIFY)) {
        log.debug(result);
      } else {
        assertEquals(invalidResponses[i], result);
      }
    }
  }

  @Test
  public void testInValidRequestsDirectBuffer() {
    String[] invalidRequests = getInvalidRequests();
    String[] invalidResponses = getInvalidResponses();
    for (int i = 0; i < invalidRequests.length; i++) {
      inDirect.clear();
      outDirect.clear();
      String inline = invalidRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      strToByteBuffer(request, inDirect);
      CommandProcessor.process(map, inDirect, outDirect);
      String result = byteBufferToString(outDirect);
      if (invalidResponses[i].equals(SKIP_VERIFY)) {
        log.debug(result);
      } else {
        assertEquals(invalidResponses[i], result);
      }
    }
  }

  @Test
  public void testInValidRequestsInlineDirectBuffer() {
    String[] invalidRequests = getInvalidRequests();
    String[] invalidResponses = getInvalidResponses();
    for (int i = 0; i < invalidRequests.length; i++) {
      inDirect.clear();
      outDirect.clear();
      String inline = invalidRequests[i];
      String request = inline;
      strToByteBuffer(request, inDirect);
      CommandProcessor.process(map, inDirect, outDirect);
      String result = byteBufferToString(outDirect);
      if (invalidResponses[i].equals(SKIP_VERIFY)) {
        log.debug(result);
      } else {
        assertEquals(invalidResponses[i], result);
      }
    }
  }

  @After
  public void tearDown() {
    map.dispose();
    DBSystem.reset();
  }
}
