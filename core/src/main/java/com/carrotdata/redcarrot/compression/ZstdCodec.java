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
package com.carrotdata.redcarrot.compression;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.carrotdata.redcarrot.redis.RedisConf;
import com.carrotdata.redcarrot.util.UnsafeAccess;
import com.carrotdata.redcarrot.util.Utils;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;



/**
 * Not sure if we need this. Codec must be 
 *
 */
public class ZstdCodec implements Codec {

  private static final Logger log = LogManager.getLogger(ZstdCodec.class);
  
  /**
   * Dictionary map dictId -> data
   */
  static ConcurrentHashMap<Integer, byte[]> dictData = new ConcurrentHashMap<Integer, byte[]>();
  
  /**
   * Compression context objects - Thread Local STorage. 
   * {dictionaryId -> compression context}
   */
  static ThreadLocal<HashMap<Integer, ZstdCompressCtx>> compContextMap =
      new ThreadLocal<HashMap<Integer, ZstdCompressCtx>>();
  
  /**
   * Decompression context objects. Thread Local Storage
   * dictionaryId -> decompression context}
   */
  static ThreadLocal<HashMap<Integer, ZstdDecompressCtx>> decompContextMap =
      new ThreadLocal<HashMap<Integer, ZstdDecompressCtx>>();
  
  /**
   * For testing
   */
  public static void reset() {
    dictData.clear();
    HashMap<Integer, ZstdCompressCtx> compContext = compContextMap.get();
    if (compContext != null) {
      for (ZstdCompressCtx v: compContext.values()) {
          v.reset();
      }
    }
    compContextMap.set(new HashMap<Integer,ZstdCompressCtx>());
    HashMap<Integer, ZstdDecompressCtx> decompContext = decompContextMap.get();
    if (decompContext != null) {
      for (ZstdDecompressCtx v: decompContext.values()) {
          v.reset();   
      }
    }
    decompContextMap.set(new HashMap<Integer,ZstdDecompressCtx>());
  }
  
  /** The min comp size. */
  private int minCompSize = 100;

  /** The total size. */
  private AtomicLong totalSize = new AtomicLong();

  /** The total comp size. */
  private AtomicLong totalCompSize = new AtomicLong();
  
  /* Dictionary size in bytes */
  private int dictSize = 1 << 16;
  
  /* Compression level */
  private int compLevel = 3;
  
  /* Dictionary enabled */
  private boolean dictionaryEnabled = true;
  
  /* Current dictionary level (maximum) */
  private volatile int currentDictVersion;
  
  /* Training in progress */
  private AtomicBoolean trainingInProgress = new AtomicBoolean(false);
  
  /**
   * Finalizing training
   */
  private AtomicBoolean finalizingTraining = new AtomicBoolean(false);
  
  /* Current size of a training data */
  private AtomicInteger trainingDataSize;
  
  /* List of data pointers for training */
  private ConcurrentLinkedQueue<Long> trainingData;
  
  /* Is dictionary training in async mode*/
  private boolean trainingAsync = true;
  
  private static volatile boolean initDone;
  
  private boolean testMode = false;
  
  public ZstdCodec() {
    try {
      init();
    } catch (IOException e) {
      log.error("Failed to initialize ZstdCodec", e);
    }
  }
  
  @Override
  public int compress(ByteBuffer src, ByteBuffer dst) throws IOException {
    int len = src.remaining();
    int offset = src.position();
    int where = dst.position();
    int dstCapacity = dst.capacity() - where;
    long srcPtr = UnsafeAccess.address(src);
    long dstPtr = UnsafeAccess.address(dst);
    int r = compress(srcPtr + offset, len, dstPtr + where, dstCapacity);
    dst.position(where);
    dst.limit(where + r);
    return r;
  }

  @Override
  public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException {
    return 0;
  }

  @Override
  public int compress(long src, int srcSize, long dst, int dstCapacity) {
    int dictId = this.currentDictVersion;
    if (isTrainingRequired()) {
      addTrainingData(src, srcSize);
    }
    ZstdCompressCtx currentCtx = getCompressContext(dictId);    
    int off = Utils.SIZEOF_SHORT;
    int compressedSize = currentCtx.compressNativeNative(dst + off, dstCapacity - off, src, srcSize);

    if (compressedSize + off >= srcSize + Utils.SIZEOF_INT) {
      return compressedSize + off;
    }
    UnsafeAccess.putShort(dst, (short) dictId);
    // update statistics
    this.totalSize.addAndGet(srcSize);
    this.totalCompSize.addAndGet(compressedSize);
    return compressedSize + off;
  }

  @Override
  public int decompress(long src, int srcSize, long dst, int dstCapacity) {
    int dictId = UnsafeAccess.toShort(src);
    src += Utils.SIZEOF_SHORT;
    ZstdDecompressCtx currentCtx = getDecompressContext(dictId);
    try {
      int decompressedSize = currentCtx.decompressNativeNative(dst, dstCapacity, src, srcSize - Utils.SIZEOF_SHORT);
      return decompressedSize;
    } catch (Throwable t) {
      log.error("dictId={} e={}", dictId, t);
      return 0;
    }
  }

  
  @Override
  public int getCompressionThreshold() {
    return minCompSize;
  }

  @Override
  public void setCompressionThreshold(int val) {
    this.minCompSize = val;
  }

  @Override
  public CodecType getType() {
    return CodecType.ZSTD;
  }

  @Override
  public double getAvgCompressionRatio() {
    long compSize = this.totalCompSize.get();
    long totalSize = this.totalSize.get();
    if (compSize == 0) return 0d;
    return (double) totalSize / compSize;
  }

  @Override
  public long getTotalBytesProcessed() {
    return totalSize.get();
  }

  @Override
  public void setLevel(int level) {
    this.compLevel = level;
  }

  @Override
  public int getLevel() {
    return this.compLevel;
  }

  private ZstdCompressCtx getCompressContext(int dictId) {
    // compression context using current dictionary id
    HashMap<Integer, ZstdCompressCtx> ctxMap = compContextMap.get();
    // This is thread local reference
    if (ctxMap == null) {
      ctxMap = new HashMap<Integer, ZstdCompressCtx>();
      Map<Integer, byte[]> dictMap = dictData;
        for (Map.Entry<Integer, byte[]> e: dictMap.entrySet()) {
          ZstdDictCompress dictCompress = new ZstdDictCompress(e.getValue(), this.compLevel);
          ZstdCompressCtx compContext = new ZstdCompressCtx();
          compContext.loadDict(dictCompress);
          compContext.setLevel(this.compLevel);
          ctxMap.put(e.getKey(), compContext);
        }
      
      compContextMap.set(ctxMap);
      // Initialize dictionary id = 0 (no dictionary)
      initCompContext(0, null);
    }
  
    // Now check the current level
    ZstdCompressCtx currentCtxt = ctxMap.get(dictId);
    if (currentCtxt == null) {
      Map<Integer, byte[]> dictMap = dictData;
      byte[] dict = dictMap.get(dictId);
      // SHOULD NOT BE NULL
      ZstdDictCompress dictCompress = new ZstdDictCompress(dict, this.compLevel);
      currentCtxt = new ZstdCompressCtx();
      currentCtxt.loadDict(dictCompress);
      currentCtxt.setLevel(this.compLevel);
      ctxMap.put(dictId, currentCtxt);
    }
    return currentCtxt;
  }
  
  private ZstdDecompressCtx getDecompressContext(int dictId) {
    // compression context using current dictionary id
    HashMap<Integer, ZstdDecompressCtx> ctxMap = decompContextMap.get();
    // This is thread local reference
    if (ctxMap == null) {
      ctxMap = new HashMap<Integer, ZstdDecompressCtx>();
      Map<Integer, byte[]> dictMap = dictData;
        for (Map.Entry<Integer, byte[]> e: dictMap.entrySet()) {
          ZstdDictDecompress dictDecompress = new ZstdDictDecompress(e.getValue());
          ZstdDecompressCtx decompContext = new ZstdDecompressCtx();
          decompContext.loadDict(dictDecompress);
          ctxMap.put(e.getKey(), decompContext);
        }

      decompContextMap.set(ctxMap);
      // Initialize dictionary id = 0 (no dictionary)
      initDecompContext(0, null);
    }
    // Now check the current level
    ZstdDecompressCtx currentCtxt = ctxMap.get(dictId);
    if (currentCtxt == null) {
      Map<Integer, byte[]> dictMap = dictData;
      byte[] dict = dictMap.get(dictId);
      if (dict == null) {
        return null; // dictionary not found
      }
      // SHOULD NOT BE NULL
      ZstdDictDecompress dictCompress = new ZstdDictDecompress(dict);
      currentCtxt = new ZstdDecompressCtx();
      currentCtxt.loadDict(dictCompress);
      ctxMap.put(this.currentDictVersion, currentCtxt);
    }
    return currentCtxt;
  }
  
  private synchronized void init() throws IOException {
    if (initDone) return;
    RedisConf config = RedisConf.getInstance();
    this.testMode = config.getTestMode();
    // TODO: config
//    this.dictSize = config.getCacheCompressionDictionarySize(cacheName);
//    this.compLevel = config.getCacheCompressionLevel(cacheName);
//    this.dictionaryEnabled = config.isCacheCompressionDictionaryEnabled(cacheName);
//    this.trainingAsync = config.isCacheCompressionDictionaryTrainingAsync(cacheName);
    String dictDir = config.getDataDir(0);
    File dir = new File(dictDir, "dict");
    if (!dir.exists()) {
      boolean result = dir.mkdirs();
      // nothing to load
      if (!result) {
        throw new IOException(String.format("Failed to create dictionary directory: %s", dictDir));
      }
      return;
    }
    loadDictionaries(dir);
    initDone = true;
  }
  
  private int getIdFromName(String name) {
    String strId = name.split("\\.")[1];
    int id = Integer.parseInt(strId);
    return id;
  }
  
  private void loadDictionaries(File dir) throws IOException {
    if (testMode) return;
    int maxId = 0;
    File[] list = dir.listFiles();
    for (File f: list) {
        byte[] data = Files.readAllBytes(Path.of(f.toURI()));
        String name = f.getName();
        int id = getIdFromName(name);
        if (id > maxId) {
          maxId = id;
        }
        dictData.put(id, data);
    }
    this.currentDictVersion = maxId;   
  }

  private void saveDictionary(int id, byte[] data) throws IOException {
    if (testMode) return;
    RedisConf config = RedisConf.getInstance();
    String dictDir = config.getDataDir(0); // temp hack
    File dir = new File(dictDir, "dict");
    dir.mkdirs();
    String name = makeDictFileName(id);
    File dictFile = new File(dir, name);
    FileOutputStream fos = new FileOutputStream(dictFile);    
    fos.write(data);
    fos.close();
  }
  
  private String makeDictFileName(int id) {
    return "dict." + id;
  }
  
  private void initCompContext(int id, byte[] dict) {
    ZstdCompressCtx compContext = new ZstdCompressCtx();
    compContext.setLevel(this.compLevel);
    HashMap<Integer, ZstdCompressCtx> ctxMap = compContextMap.get();
    if (ctxMap == null) {
      ctxMap = new HashMap<Integer, ZstdCompressCtx> ();
      compContextMap.set(ctxMap);
    }
    if (dict == null) {
      id = 0;
    } else {
      ZstdDictCompress dictCompress = new ZstdDictCompress(dict, this.compLevel);
      compContext.loadDict(dictCompress);
    }
    ctxMap.put(id, compContext);
  }
  
  private void initDecompContext(int id, byte[] dict) {
    ZstdDecompressCtx decompContext = new ZstdDecompressCtx();
    HashMap<Integer, ZstdDecompressCtx> ctxMap = decompContextMap.get();
    if (ctxMap == null) {
      ctxMap = new HashMap<Integer, ZstdDecompressCtx> ();
      decompContextMap.set(ctxMap);
    }
    if (dict == null) {
      id = 0;
    } else {
      ZstdDictDecompress dictCompress = new ZstdDictDecompress(dict);
      decompContext.loadDict(dictCompress);
    }
    ctxMap.put(id, decompContext);
  }

  private synchronized void startTraining() {
    if (this.trainingInProgress.get()) return;
    boolean success = this.trainingInProgress.compareAndSet(false, true);
    if (!success) {
      return;
    }
    log.debug("Start training");
    this.trainingDataSize = new AtomicInteger();
    this.trainingData = new ConcurrentLinkedQueue<Long>();
  }

  public synchronized void addTrainingData(long ptr, int size) {
    if (!trainingInProgress.get() || finalizingTraining.get()) {
      return;
    }
    // There is the issue with the implementation
    // we add to the training set data blocks not data items
    // it means that there are too many duplicates in samples
    // therefore we add probability element
    ThreadLocalRandom r = ThreadLocalRandom.current();
    double d = r.nextDouble();
    if (d > 0.01d) return;
    long $ptr = UnsafeAccess.malloc(size + Utils.SIZEOF_INT);
    UnsafeAccess.copy(ptr, $ptr + Utils.SIZEOF_INT, size);
    UnsafeAccess.putInt($ptr,  size);
    this.trainingData.add($ptr);
    this.trainingDataSize.addAndGet(size);
    checkFinishTraining();
  }
  
  public boolean isTrainingRequired() {
    if (!this.dictionaryEnabled) {
      return false;
    }
    boolean required = this.currentDictVersion == 0;
    if (required && !this.trainingInProgress.get()) {
      startTraining();
    }
    return required;
  }

  private int getRecommendedTrainingDataSize() {
    return 100 * this.dictSize;
  }
  
  private void checkFinishTraining() {
    if (this.dictionaryEnabled && this.trainingDataSize.get() >= getRecommendedTrainingDataSize()) {
      finishTraining();
    }
  }
  
  @SuppressWarnings("unused")
  private void finishTraining() {
    if (! finalizingTraining.compareAndSet(false, true)) {
      return;
    }

    Runnable r = () -> {
      byte[] dict;
      ZstdDictTrainer trainer = new ZstdDictTrainer(this.trainingDataSize.get(), this.dictSize);
      for (Long ptr: this.trainingData) {
        int size = UnsafeAccess.toInt(ptr);
        byte[] data = new byte[size];
        UnsafeAccess.copy(ptr + Utils.SIZEOF_INT, data, 0, size);
        trainer.addSample(data);
      }
      long start = System.currentTimeMillis();
      dict = trainer.trainSamples();
      long end = System.currentTimeMillis();
      Map<Integer, byte[]> dictMap = dictData;
      dictMap.put(this.currentDictVersion + 1, dict);
      this.currentDictVersion++;
      // Deallocate resources
      this.trainingDataSize.set(0);
      while(!this.trainingData.isEmpty()) {
        long ptr = this.trainingData.poll();
        UnsafeAccess.free(ptr);
      }
      this.trainingInProgress.set(false);
      this.finalizingTraining.set(false);
      try {
        saveDictionary(this.currentDictVersion, dict);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      log.debug("Finished training in {} ms", System.currentTimeMillis() - start);
    };
    if (this.trainingAsync) {
      // Run training session
      new Thread(r).start();
    } else {
      r.run();
    }
  }
}
