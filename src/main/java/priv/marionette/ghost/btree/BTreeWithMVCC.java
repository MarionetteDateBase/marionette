package priv.marionette.ghost.btree;

import priv.marionette.cache.LIRSCache;
import priv.marionette.compress.Compressor;
import priv.marionette.ghost.*;
import priv.marionette.ghost.type.StringDataType;
import priv.marionette.tools.DataUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
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


    BTreeWithMVCC(Map<String, Object> config){
        this.compressionLevel = DataUtils.getConfigParam(config, "compress", 0);
        String fileName = (String) config.get("fileName");
        FileStore fileStore = (FileStore) config.get("fileStore");
        fileStoreIsProvided = fileStore != null;
        if(fileStore == null && fileName != null) {
            fileStore = new FileStore();
        }
        this.fileStore = fileStore;

        int pgSplitSize = 48; // for "mem:" case it is # of keys
        LIRSCache.Config cc = null;
        if (this.fileStore != null) {
            int mb = DataUtils.getConfigParam(config, "cacheSize", 16);
            if (mb > 0) {
                cc = new LIRSCache.Config();
                cc.maxMemory = mb * 1024L * 1024L;
                Object o = config.get("cacheConcurrency");
                if (o != null) {
                    cc.segmentCount = (Integer)o;
                }
            }
            pgSplitSize = 16 * 1024;
        }
        if (cc != null) {
            cache = new LIRSCache<>(cc);
            cc.maxMemory /= 4;
            cacheChunkRef = new LIRSCache<>(cc);
        } else {
            cache = null;
            cacheChunkRef = null;
        }

        pgSplitSize = DataUtils.getConfigParam(config, "pageSplitSize", pgSplitSize);
        // Make sure pages will fit into cache
        if (cache != null && pgSplitSize > cache.getMaxItemSize()) {
            pgSplitSize = (int)cache.getMaxItemSize();
        }
        pageSplitSize = pgSplitSize;
        backgroundExceptionHandler =
                (Thread.UncaughtExceptionHandler)config.get("backgroundExceptionHandler");
        meta = new MVMap<>(StringDataType.INSTANCE,
                StringDataType.INSTANCE);
        HashMap<String, Object> c = new HashMap<>();
        c.put("id", 0);
        c.put("createVersion", currentVersion);
        meta.init(this, c);
        if (this.fileStore != null) {
            retentionTime = this.fileStore.getDefaultRetentionTime();
            int kb = DataUtils.getConfigParam(config, "autoCommitBufferSize", 1024);
            // 内存数据与序列化后数据大小之比约为19:1
            autoCommitMemory = kb * 1024 * 19;
            autoCompactFillRate = DataUtils.getConfigParam(config, "autoCompactFillRate", 50);
            char[] encryptionKey = (char[]) config.get("encryptionKey");
            try {
                if (!fileStoreIsProvided) {
                    boolean readOnly = config.containsKey("readOnly");
                    this.fileStore.open(fileName, readOnly, encryptionKey);
                }
                if (this.fileStore.size() == 0) {
                    creationTime = getTimeAbsolute();
                    lastCommitTime = creationTime;
                    storeHeader.put("M", 2);
                    storeHeader.put("blockSize", BLOCK_SIZE);
                    storeHeader.put("format", FORMAT_WRITE);
                    storeHeader.put("created", creationTime);
                    writeStoreHeader();
                } else {
                    readStoreHeader();
                }
            } catch (IllegalStateException e) {
                panic(e);
            } finally {
                if (encryptionKey != null) {
                    Arrays.fill(encryptionKey, (char) 0);
                }
            }
            lastCommitTime = getTimeSinceCreation();

            // setAutoCommitDelay starts the thread, but only if
            // the parameter is different from the old value
            int delay = DataUtils.getConfigParam(config, "autoCommitDelay", 1000);
            setAutoCommitDelay(delay);
        } else {
            autoCommitMemory = 0;
            autoCompactFillRate = 0;
        }

    }



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

    private long getTimeSinceCreation() {
        return Math.max(0, getTimeAbsolute() - creationTime);
    }

    private long getTimeAbsolute() {
        long now = System.currentTimeMillis();
        if (lastTimeAbsolute != 0 && now < lastTimeAbsolute) {
            // 避免由于闰秒导致系统时间错误
            now = lastTimeAbsolute;
        } else {
            lastTimeAbsolute = now;
        }
        return now;
    }

    private void writeStoreHeader() {
        StringBuilder buff = new StringBuilder();
        if (lastChunk != null) {
            storeHeader.put("block", lastChunk.block);
            storeHeader.put("chunk", lastChunk.id);
            storeHeader.put("version", lastChunk.version);
        }
        DataUtils.appendMap(buff, storeHeader);
        byte[] bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
        int checksum = DataUtils.getFletcher32(bytes, 0, bytes.length);
        DataUtils.appendMap(buff, "fletcher", checksum);
        buff.append("\n");
        bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
        ByteBuffer header = ByteBuffer.allocate(2 * BLOCK_SIZE);
        header.put(bytes);
        header.position(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        write(0, header);
    }

    private void write(long pos, ByteBuffer buffer) {
        try {
            fileStore.writeFully(pos, buffer);
        } catch (IllegalStateException e) {
            panic(e);
            throw e;
        }
    }

    private void panic(IllegalStateException e) {
        handleException(e);
        panicException = e;
        closeImmediately();
        throw e;
    }

    private void handleException(Throwable ex) {
        if (backgroundExceptionHandler != null) {
            try {
                backgroundExceptionHandler.uncaughtException(null, ex);
            } catch(Throwable ignore) {
                if (ex != ignore) { // OOME may be the same
                    ex.addSuppressed(ignore);
                }
            }
        }
    }

    public void closeImmediately() {
        try {
            closeStore(false);
        } catch (Throwable e) {
            handleException(e);
        }
    }





    private void closeStore(boolean shrinkIfPossible) {
        if (closed) {
            return;
        }
        // can not synchronize on this yet, because
        // the thread also synchronized on this, which
        // could result in a deadlock
        stopBackgroundThread();
        closed = true;
        synchronized (this) {
            if (fileStore != null && shrinkIfPossible) {
                shrinkFileIfPossible(0);
            }
            // release memory early - this is important when called
            // because of out of memory
            if (cache != null) {
                cache.clear();
            }
            if (cacheChunkRef != null) {
                cacheChunkRef.clear();
            }
            for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
                m.close();
            }
            chunks.clear();
            maps.clear();
            if (fileStore != null && !fileStoreIsProvided) {
                fileStore.close();
            }
        }
    }

    private void stopBackgroundThread() {
        BackgroundWriterThread t = backgroundWriterThread;
        if (t == null) {
            return;
        }
        backgroundWriterThread = null;
        if (Thread.currentThread() == t) {
            // within the thread itself - can not join
            return;
        }
        synchronized (t.sync) {
            t.sync.notifyAll();
        }
        if (Thread.holdsLock(this)) {
            // called from storeNow: can not join,
            // because that could result in a deadlock
            return;
        }
        try {
            t.join();
        } catch (Exception e) {
            // ignore
        }
    }

    public void close() {
        if (closed) {
            return;
        }
        FileStore f = fileStore;
        if (f != null && !f.isReadOnly()) {
            stopBackgroundThread();
            if (hasUnsavedChanges()) {
                commitAndSave();
            }
        }
        closeStore(true);
    }

    public boolean hasUnsavedChanges() {
        checkOpen();
        if (metaChanged) {
            return true;
        }
        for (MVMap<?, ?> m : maps.values()) {
            if (!m.isClosed()) {
                long v = m.getVersion();
                if (v >= 0 && v > lastStoredVersion) {
                    return true;
                }
            }
        }
        return false;
    }

    private void checkOpen() {
        if (closed) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED,
                    "This store is closed", panicException);
        }
    }



    private synchronized long commitAndSave() {
        if (closed) {
            return currentVersion;
        }
        if (fileStore == null) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_WRITING_FAILED,
                    "This is an in-memory store");
        }
        if (currentStoreVersion >= 0) {
            // store is possibly called within store, if the meta map changed
            return currentVersion;
        }
        if (!hasUnsavedChanges()) {
            return currentVersion;
        }
        if (fileStore.isReadOnly()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_WRITING_FAILED, "This store is read-only");
        }
        try {
            currentStoreVersion = currentVersion;
            currentStoreThread = Thread.currentThread();
            return storeNow();
        } finally {
            // in any case reset the current store version,
            // to allow closing the store
            currentStoreVersion = -1;
            currentStoreThread = null;
        }
    }


    private long storeNow() {
        try {
            return storeNowTry();
        } catch (IllegalStateException e) {
            panic(e);
            return -1;
        }
    }


    private long storeNowTry(){
        long time = getTimeSinceCreation();
        int freeDelay = retentionTime / 10;
        if (time >= lastFreeUnusedChunks + freeDelay) {
            // set early in case it fails (out of memory or so)
            lastFreeUnusedChunks = time;
            freeUnusedChunks();
            // set it here as well, to avoid calling it often if it was slow
            lastFreeUnusedChunks = getTimeSinceCreation();
        }
    }

    private synchronized void freeUnusedChunks() {
        if (lastChunk == null || !reuseSpace) {
            return;
        }
        Set<Integer> referenced = collectReferencedChunks();
        long time = getTimeSinceCreation();

        for (Iterator<Chunk> it = chunks.values().iterator(); it.hasNext(); ) {
            Chunk c = it.next();
            if (!referenced.contains(c.id)) {
                if (canOverwriteChunk(c, time)) {
                    it.remove();
                    markMetaChanged();
                    meta.remove(Chunk.getMetaKey(c.id));
                    long start = c.block * BLOCK_SIZE;
                    int length = c.len * BLOCK_SIZE;
                    fileStore.free(start, length);
                } else {
                    if (c.unused == 0) {
                        c.unused = time;
                        meta.put(Chunk.getMetaKey(c.id), c.asString());
                        markMetaChanged();
                    }
                }
            }
        }
    }

    private Set<Integer> collectReferencedChunks() {
        long testVersion = lastChunk.version;
        DataUtils.checkArgument(testVersion > 0, "Collect references on version 0");
        long readCount = getFileStore().readCount.get();
        Set<Integer> referenced = new HashSet<>();
        for (Cursor<String, String> c = meta.cursor("root."); c.hasNext();) {
            String key = c.next();
            if (!key.startsWith("root.")) {
                break;
            }
            long pos = DataUtils.parseHexLong(c.getValue());
            if (pos == 0) {
                continue;
            }
            int mapId = DataUtils.parseHexInt(key.substring("root.".length()));
            collectReferencedChunks(referenced, mapId, pos, 0);
        }
        long pos = lastChunk.metaRootPos;
        collectReferencedChunks(referenced, 0, pos, 0);
        readCount = fileStore.readCount.get() - readCount;
        return referenced;
    }








    private static class BackgroundWriterThread extends Thread {
        public final Object sync = new Object();

    }





}
