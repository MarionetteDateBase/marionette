package priv.marionette.ghost.btree;

import priv.marionette.cache.LIRSCache;
import priv.marionette.compress.Compressor;
import priv.marionette.ghost.*;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 以B树为单位，以MVCC作为并发控制的<K,V>式数据存储
 *
 * @author Yue Yu
 * @create 2018-03-13 下午4:06
 **/
public final class BTreeWithMVCC {

    public static final boolean ASSERT = false;

    /**
     * 区块大小，一个Chunk的有两个header，第二个是第一个header的备份
     */
    public static final int BLOCK_SIZE = 4 * 1024;

    private static final int FORMAT_WRITE = 1;

    private static final int FORMAT_READ = 1;

    /**
     * 强制垃圾回收
     */
    private static final int MARKED_FREE = 10000000;

    /**
     * 持续持久化内存数据更改的master线程，类似于mongodb的fork后台子进程
     */
    volatile BackgroundWriterThread backgroundWriterThread;

    private volatile boolean reuseSpace = true;

    private volatile boolean closed;

    private final FileStore fileStore;

    private final boolean fileStoreIsProvided;

    private final int pageSplitSize;

    /**
     * 页面置换缓存
     */
    private final LIRSCache<Page> cache;


    private final LIRSCache<Page.PageChildren> cacheChunkRef;

    private Chunk lastChunk;

    private final ConcurrentHashMap<Integer, Chunk> chunks =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Long,
            ConcurrentHashMap<Integer, Chunk>> freedPageSpace =
            new ConcurrentHashMap<>();

    private final MVMap<String, String> meta;

    private final ConcurrentHashMap<Integer, MVMap<?, ?>> maps =
            new ConcurrentHashMap<>();

    private final HashMap<String, Object> storeHeader = new HashMap<>();

    private WriteBuffer writeBuffer;

    private int lastMapId;

    private int versionsToKeep = 5;

    private final int compressionLevel;

    private Compressor compressorFast;

    private Compressor compressorHigh;

    private final Thread.UncaughtExceptionHandler backgroundExceptionHandler;

    private volatile long currentVersion;

    private long lastStoredVersion;

    private int unsavedMemory;
    private final int autoCommitMemory;
    private boolean saveNeeded;

    private long creationTime;

    private int retentionTime;

    private long lastCommitTime;

    private Chunk retainChunk;

    private volatile long currentStoreVersion = -1;

    private Thread currentStoreThread;

    private volatile boolean metaChanged;

    private int autoCommitDelay;

    private final int autoCompactFillRate;

    private long autoCompactLastFileOpCount;

    private final Object compactSync = new Object();

    private IllegalStateException panicException;

    private long lastTimeAbsolute;

    private long lastFreeUnusedChunks;


    public long getCurrentVersion() {
        return currentVersion;
    }

    public FileStore getFileStore() {
        return fileStore;
    }


    public void registerUnsavedPage(int memory) {
        unsavedMemory += memory;
        int newValue = unsavedMemory;
        if (newValue > autoCommitMemory && autoCommitMemory > 0) {
            saveNeeded = true;
        }
    }

    private static class BackgroundWriterThread extends Thread {


    }





}
