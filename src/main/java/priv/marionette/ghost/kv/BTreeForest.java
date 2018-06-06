package priv.marionette.ghost.kv;

import priv.marionette.cache.LIRSCache;
import priv.marionette.compress.CompressDeflate;
import priv.marionette.compress.CompressLZF;
import priv.marionette.compress.Compressor;
import priv.marionette.engine.Constants;
import priv.marionette.ghost.*;
import priv.marionette.tools.DataUtils;
import priv.marionette.tools.MathUtils;
import priv.marionette.tools.New;
import static priv.marionette.ghost.kv.MVBTreeMap.INITIAL_VERSION;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * 以B树为单位，以MVCC作为并发控制的<K,V>式数据存储的森林
 *
 * @author Yue Yu
 * @create 2018-03-13 下午4:06
 **/
public final class BTreeForest {

    /**
     * 区块大小，一个Chunk的有两个header，第二个是第一个header的备份
     */
    public static final int BLOCK_SIZE = 4 * 1024;

    private static final int FORMAT_WRITE = 1;

    private static final int FORMAT_READ = 1;

    /**
     * 强制垃圾回收
     */
    private static final int MARKED_FREE = 10_000_000;

    /**
     * 持续持久化内存数据更改的master线程，类似于mongodb的fork后台子进程
     */
    volatile BackgroundWriterThread backgroundWriterThread;

    private volatile boolean reuseSpace = true;

    private volatile boolean closed;

    final FileStore fileStore;
    private final boolean fileStoreIsProvided;

    private final int pageSplitSize;

    private final int keysPerPage;

    /**
     * 页面置换缓存
     */
    private final LIRSCache<Page> cache;


    private final LIRSCache<int[]> cacheChunkRef;

    private Chunk lastChunk;

    private final ConcurrentHashMap<Integer, Chunk> chunks =
            new ConcurrentHashMap<>();

    private long updateCounter = 0;
    private long updateAttemptCounter = 0;

    /**
     * 记录被回收的freed chunks
     */
    private final Map<Integer, Chunk> freedPageSpace = new HashMap<>();

    private final MVBTreeMap<String, String> meta;

    private final ConcurrentHashMap<Integer, MVBTreeMap<?, ?>> maps =
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

    /**
     * 最新的chunk持久化版本，如果没有持久化操作则为-1
     */
    private long lastStoredVersion = INITIAL_VERSION;

    /**
     * 正在使用中的最旧版本号，在此之前的数据版本可以无条件释放
     */
    private final AtomicLong oldestVersionToKeep = new AtomicLong();

    /**
     * 在开启事务性操作后记录所有新的版本
     */
    private final Deque<TxCounter> versions = new LinkedList<>();

    /**
     * 当前版本上正在进行的事务性操作数
     */
    private volatile TxCounter currentTxCounter = new TxCounter(currentVersion);

    /**
     * 非持久化的page所需要占用的最基本内存大小(估算)
     */
    private int unsavedMemory;
    private final int autoCommitMemory;
    private volatile boolean saveNeeded;

    /**
     * 创建时间(unix时间戳)
     */
    private long creationTime;

    /**
     * 版本废弃延迟时间
     */
    private int retentionTime;

    private long lastCommitTime;

    /**
     * 当前持久化操作版本
     */
    private volatile long currentStoreVersion = -1;

    /**
     * 正在进行持久化的线程的引用
     */
    private final AtomicReference<Thread> currentStoreThread = new AtomicReference<>();

    private volatile boolean metaChanged;

    /**
     * autoCommit延迟
     */
    private int autoCommitDelay;

    private final int autoCompactFillRate;
    private long autoCompactLastFileOpCount;

    private final Object compactSync = new Object();

    private IllegalStateException panicException;

    private long lastTimeAbsolute;

    private long lastFreeUnusedChunks;



    BTreeForest(Map<String, Object> config){
        this.compressionLevel = DataUtils.getConfigParam(config, "compress", 0);
        String fileName = (String) config.get("fileName");
        FileStore fileStore = (FileStore) config.get("fileStore");
        fileStoreIsProvided = fileStore != null;
        if(fileStore == null && fileName != null) {
            fileStore = new FileStore();
        }
        this.fileStore = fileStore;

        int pgSplitSize = 48;
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
        keysPerPage = DataUtils.getConfigParam(config, "keysPerPage", 48);
        backgroundExceptionHandler =
                (Thread.UncaughtExceptionHandler)config.get("backgroundExceptionHandler");
        meta = new MVBTreeMap<>(this);
        meta.init();
        if (this.fileStore != null) {
            retentionTime = this.fileStore.getDefaultRetentionTime();
            int kb = DataUtils.getConfigParam(config, "autoCommitBufferSize", 1024);
            // 内存与持久化字节用量为19：1
            autoCommitMemory = kb * 1024 * 19;
            autoCompactFillRate = DataUtils.getConfigParam(config, "autoCompactFillRate", 40);
            char[] encryptionKey = (char[]) config.get("encryptionKey");
            try {
                if (!fileStoreIsProvided) {
                    boolean readOnly = config.containsKey("readOnly");
                    this.fileStore.open(fileName, readOnly, encryptionKey);
                }
                if (this.fileStore.size() == 0) {
                    creationTime = getTimeAbsolute();
                    lastCommitTime = creationTime;
                    storeHeader.put("H", 2);
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

            Set<String> rootsToRemove = new HashSet<>();
            for (Iterator<String> it = meta.keyIterator("root."); it.hasNext();) {
                String key = it.next();
                if (!key.startsWith("root.")) {
                    break;
                }
                String mapId = key.substring(key.lastIndexOf('.') + 1);
                if(!meta.containsKey("map."+mapId)) {
                    rootsToRemove.add(key);
                }
            }

            for (String key : rootsToRemove) {
                meta.remove(key);
                markMetaChanged();
            }

            int delay = DataUtils.getConfigParam(config, "autoCommitDelay", 1000);
            setAutoCommitDelay(delay);
        } else {
            autoCommitMemory = 0;
            autoCompactFillRate = 0;
        }

    }

    public void setAutoCommitDelay(int millis) {
        if (autoCommitDelay == millis) {
            return;
        }
        autoCommitDelay = millis;
        if (fileStore == null || fileStore.isReadOnly()) {
            return;
        }
        stopBackgroundThread();
        if (millis > 0) {
            int sleep = Math.max(1, millis / 10);
            BackgroundWriterThread t =
                    new BackgroundWriterThread(this, sleep,
                            fileStore.toString());
            t.start();
            backgroundWriterThread = t;
        }
    }

    /**
     * 以独占模式访问BP树
     * @param fileName
     * @return
     */
    public static BTreeForest open(String fileName) {
        HashMap<String, Object> config = new HashMap<>();
        config.put("fileName", fileName);
        return new BTreeForest(config);
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    public FileStore getFileStore() {
        return fileStore;
    }

    public int getKeysPerPage() {
        return keysPerPage;
    }

    public long getMaxPageSize() {
        return cache == null ? Long.MAX_VALUE : cache.getMaxItemSize() >> 4;
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
        if (!closed) {
            handleException(e);
            panicException = e;
            closeImmediately();
        }
        throw e;
    }

    public IllegalStateException getPanicException() {
        return panicException;
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
        stopBackgroundThread();
        closed = true;
        synchronized (this) {
            if (fileStore != null && shrinkIfPossible) {
                shrinkFileIfPossible(0);
            }
            // 释放内存
            if (cache != null) {
                cache.clear();
            }
            if (cacheChunkRef != null) {
                cacheChunkRef.clear();
            }
            for (MVBTreeMap<?, ?> m : new ArrayList<>(maps.values())) {
                //is marked as closed
                m.close();
            }
            chunks.clear();
            maps.clear();
            if (fileStore != null && !fileStoreIsProvided) {
                fileStore.close();
            }
        }
    }

    /**
     * 收缩文件长度
     * @param minPercent
     */
    private void shrinkFileIfPossible(int minPercent) {
        if (fileStore.isReadOnly()) {
            return;
        }
        long end = getFileLengthInUse();
        long fileSize = fileStore.size();
        if (end >= fileSize) {
            return;
        }
        if (minPercent > 0 && fileSize - end < BLOCK_SIZE) {
            return;
        }
        int savedPercent = (int) (100 - (end * 100 / fileSize));
        if (savedPercent < minPercent) {
            return;
        }
        //如果文件流未关闭，将所有更改强制同步到磁盘
        if (!closed) {
            sync();
        }
        fileStore.truncate(end);
    }

    public void sync() {
        checkOpen();
        FileStore f = fileStore;
        if (f != null) {
            f.sync();
        }
    }

    private long getFileLengthInUse() {
        long result = fileStore.getFileLengthInUse();
        assert result == _getFileLengthInUse() : result + " != " + _getFileLengthInUse();
        return result;
    }

    private long _getFileLengthInUse() {
        long size = 2;
        for (Chunk c : chunks.values()) {
            if (c.len != Integer.MAX_VALUE) {
                size = Math.max(size, c.block + c.len);
            }
        }
        return size * BLOCK_SIZE;
    }



    private void stopBackgroundThread() {
        BackgroundWriterThread t = backgroundWriterThread;
        if (t == null) {
            return;
        }
        backgroundWriterThread = null;
        if (Thread.currentThread() == t) {
            return;
        }
        synchronized (t.sync) {
            t.sync.notifyAll();
        }
        if (Thread.holdsLock(this)) {
            // 如果正在进行持久化则放弃stop，避免造成死锁
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
            for (MVBTreeMap<?, ?> map : maps.values()) {
                if (map.isClosed()) {
                    if (meta.remove(MVBTreeMap.getMapRootKey(map.getId())) != null) {
                        markMetaChanged();
                    }
                }
            }
            commit();
        }
        closeStore(true);
    }

    public boolean hasUnsavedChanges() {
        checkOpen();
        if (metaChanged) {
            return true;
        }
        for (MVBTreeMap<?, ?> m : maps.values()) {
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

    /**
     * 在写map之前先尝试commit已有的数据
     * @param map
     */
    void beforeWrite(MVBTreeMap<?, ?> map) {
        if (saveNeeded && fileStore != null && !closed && autoCommitDelay > 0) {
            saveNeeded = false;
            // check again, because it could have been written by now
            if (unsavedMemory > autoCommitMemory && autoCommitMemory > 0) {
                tryCommit();
            }
        }
    }

    /**
     * 在autoCommit开启后，定时或内存数据超出阀值时，对数据更改做持久化操作
     * @return
     */
    public synchronized long commit() {
        currentStoreThread.set(Thread.currentThread());
        store();
        return currentVersion;
    }

    public long tryCommit() {
        if (currentStoreThread.compareAndSet(null, Thread.currentThread())) {
            synchronized (this) {
                store();
            }
        }
        return currentVersion;
    }

    private void store() {
        try {
            if (!closed && hasUnsavedChangesInternal()) {
                currentStoreVersion = currentVersion;
                if (fileStore == null) {
                    lastStoredVersion = currentVersion;
                    ++currentVersion;
                    setWriteVersion(currentVersion);
                    metaChanged = false;
                } else {
                    if (fileStore.isReadOnly()) {
                        throw DataUtils.newIllegalStateException(
                                DataUtils.ERROR_WRITING_FAILED, "This store is read-only");
                    }
                    try {
                        storeNow();
                    } catch (IllegalStateException e) {
                        panic(e);
                    } catch (Throwable e) {
                        panic(DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, e.toString(), e));
                    }
                }
            }
        } finally {
            currentStoreVersion = -1;
            currentStoreThread.set(null);
        }
    }

    private boolean hasUnsavedChangesInternal() {
        if (meta.hasChangesSince(lastStoredVersion)) {
            return true;
        }
        return hasUnsavedChanges();
    }

    public int getPageSplitSize() {
        return pageSplitSize;
    }


    private void storeNow() {
        assert Thread.holdsLock(this);
        long time = getTimeSinceCreation();
        freeUnusedIfNeeded(time);
        int currentUnsavedPageCount = unsavedMemory;
        long storeVersion = currentStoreVersion;
        long version = ++currentVersion;
        lastCommitTime = time;
        int lastChunkId;
        if (lastChunk == null) {
            lastChunkId = 0;
        } else {
            lastChunkId = lastChunk.id;
            meta.put(Chunk.getMetaKey(lastChunkId), lastChunk.asString());
            markMetaChanged();
            time = Math.max(lastChunk.time, time);
        }
        int newChunkId = lastChunkId;
        for(;;) {
            newChunkId = (newChunkId + 1) & Chunk.MAX_ID;
            Chunk old = chunks.get(newChunkId);
            if (old == null) {
                break;
            }
            if (old.block == Long.MAX_VALUE) {
                IllegalStateException e = DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Last block {0} not stored, possibly due to out-of-memory", old);
                panic(e);
            }
        }
        Chunk c = new Chunk(newChunkId);
        c.pageCount = Integer.MAX_VALUE;
        c.pageCountLive = Integer.MAX_VALUE;
        c.maxLen = Long.MAX_VALUE;
        c.maxLenLive = Long.MAX_VALUE;
        c.metaRootPos = Long.MAX_VALUE;
        c.block = Long.MAX_VALUE;
        c.len = Integer.MAX_VALUE;
        c.time = time;
        c.version = version;
        c.mapId = lastMapId;
        c.next = Long.MAX_VALUE;
        chunks.put(c.id, c);
        // force a metadata update
        meta.put(Chunk.getMetaKey(c.id), c.asString());
        meta.remove(Chunk.getMetaKey(c.id));
        markMetaChanged();
        ArrayList<Page> changed = new ArrayList<>();
        for (Iterator<MVBTreeMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVBTreeMap<?, ?> map = iter.next();
            MVBTreeMap.RootReference rootReference = map.setWriteVersion(version);
            if (rootReference == null) {
                assert map.isClosed();
                assert map.getVersion() < getOldestVersionToKeep();
                meta.remove(MVBTreeMap.getMapRootKey(map.getId()));
                iter.remove();
            } else if (map.getCreateVersion() <= storeVersion && // if map was created after storing started, skip it
                    !map.isVolatile() &&
                    map.hasChangesSince(lastStoredVersion)) {
                assert rootReference.version <= version : rootReference.version + " > " + version;
                Page rootPage = rootReference.root;
                if (!rootPage.isSaved() ||
                        // after deletion previously saved leaf
                        // may pop up as a root, but we still need
                        // to save new root pos in meta
                        rootPage.isLeaf()) {
                    changed.add(rootPage);
                }
            }
        }
        WriteBuffer buff = getWriteBuffer();
        // need to patch the header later
        c.writeChunkHeader(buff, 0);
        int headerLength = buff.position();
        c.pageCount = 0;
        c.pageCountLive = 0;
        c.maxLen = 0;
        c.maxLenLive = 0;
        for (Page p : changed) {
            String key = MVBTreeMap.getMapRootKey(p.getMapId());
            if (p.getTotalCount() == 0) {
                meta.remove(key);
            } else {
                p.writeUnsavedRecursive(c, buff);
                long root = p.getPos();
                meta.put(key, Long.toHexString(root));
            }
        }
        applyFreedSpace();
        MVBTreeMap.RootReference metaRootReference = meta.setWriteVersion(version);
        assert metaRootReference != null;
        assert metaRootReference.version == version : metaRootReference.version + " != " + version;
        metaChanged = false;
        onVersionChange(version);

        Page metaRoot = metaRootReference.root;
        metaRoot.writeUnsavedRecursive(c, buff);

        int chunkLength = buff.position();

        // 将limit设置到下一个block位置
        int length = MathUtils.roundUpInt(chunkLength +
                Chunk.FOOTER_LENGTH, BLOCK_SIZE);
        buff.limit(length);

        long filePos = allocateFileSpace(length, !reuseSpace);
        c.block = filePos / BLOCK_SIZE;
        c.len = length / BLOCK_SIZE;
        assert fileStore.getFileLengthInUse() == measureFileLengthInUse() :
                fileStore.getFileLengthInUse() + " != " + measureFileLengthInUse() + " " + c;
        c.metaRootPos = metaRoot.getPos();
        //对下一个chunk的位置做预分配
        if (reuseSpace) {
            c.next = fileStore.predictAllocation(c.len * BLOCK_SIZE) / BLOCK_SIZE;
        } else {
            c.next = 0;
        }
        buff.position(0);
        c.writeChunkHeader(buff, headerLength);

        buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
        buff.put(c.getFooterBytes());

        buff.position(0);
        write(filePos, buff.getBuffer());
        releaseWriteBuffer(buff);

        // whether we need to write the store header
        boolean writeStoreHeader = false;
        // end of the used space is not necessarily the end of the file
        boolean storeAtEndOfFile = filePos + length >= fileStore.size();
        if (!storeAtEndOfFile) {
            if (lastChunk == null) {
                writeStoreHeader = true;
            } else if (lastChunk.next != c.block) {
                // the last prediction did not matched
                writeStoreHeader = true;
            } else {
                long headerVersion = DataUtils.readHexLong(
                        storeHeader, "version", 0);
                if (lastChunk.version - headerVersion > 20) {
                    // we write after at least every 20 versions
                    writeStoreHeader = true;
                } else {
                    int chunkId = DataUtils.readHexInt(storeHeader, "chunk", 0);
                    while (true) {
                        Chunk old = chunks.get(chunkId);
                        if (old == null) {
                            // one of the chunks in between
                            // was removed
                            writeStoreHeader = true;
                            break;
                        }
                        if (chunkId == lastChunk.id) {
                            break;
                        }
                        chunkId++;
                    }
                }
            }
        }


        lastChunk = c;
        if (writeStoreHeader) {
            writeStoreHeader();
        }
        if (!storeAtEndOfFile) {
            // may only shrink after the store header was written
            shrinkFileIfPossible(1);
        }
        for (Page p : changed) {
            if (p.getTotalCount() > 0) {
                p.writeEnd();
            }
        }
        metaRoot.writeEnd();

        // some pages might have been changed in the meantime (in the newest
        // version)
        unsavedMemory = Math.max(0, unsavedMemory
                - currentUnsavedPageCount);

        lastStoredVersion = storeVersion;
    }

    private long allocateFileSpace(int length, boolean atTheEnd) {
        long filePos;
        if (atTheEnd) {
            filePos = getFileLengthInUse();
            fileStore.markUsed(filePos, length);
        } else {
            filePos = fileStore.allocate(length);
        }
        return filePos;
    }

    private long measureFileLengthInUse() {
        long size = 2;
        for (Chunk c : chunks.values()) {
            if (c.len != Integer.MAX_VALUE) {
                size = Math.max(size, c.block + c.len);
            }
        }
        return size * BLOCK_SIZE;
    }

    /**
     * 取回被回收的可用空间
     */
    private void applyFreedSpace() {
        while (true) {
            ArrayList<Chunk> modified = new ArrayList<>();
            synchronized (freedPageSpace) {
                for (Chunk f : freedPageSpace.values()) {
                    Chunk c = chunks.get(f.id);
                    if (c != null) {
                        c.maxLenLive += f.maxLenLive;
                        c.pageCountLive += f.pageCountLive;
                        if (c.pageCountLive < 0 && c.pageCountLive > -MARKED_FREE) {
                            c.pageCountLive = 0;
                        }
                        if (c.maxLenLive < 0 && c.maxLenLive > -MARKED_FREE) {
                            c.maxLenLive = 0;
                        }
                        modified.add(c);
                    }
                }
                freedPageSpace.clear();
            }
            for (Chunk c : modified) {
                meta.put(Chunk.getMetaKey(c.id), c.asString());
            }
            if (modified.isEmpty()) {
                break;
            }
            markMetaChanged();
        }
    }


    private void onVersionChange(long version) {
        TxCounter txCounter = this.currentTxCounter;
        assert txCounter.counter.get() >= 0;
        versions.add(txCounter);
        currentTxCounter = new TxCounter(version);
        txCounter.counter.decrementAndGet();
        dropUnusedVersions();
    }

    private void dropUnusedVersions() {
        TxCounter txCounter;
        while ((txCounter = versions.peek()) != null
                && txCounter.counter.get() < 0) {
            versions.poll();
        }
        setOldestVersionToKeep(txCounter != null ? txCounter.version : currentTxCounter.version);
    }

    private void setOldestVersionToKeep(long oldestVersionToKeep) {
        boolean success;
        do {
            long current = this.oldestVersionToKeep.get();
            success = oldestVersionToKeep <= current ||
                    this.oldestVersionToKeep.compareAndSet(current, oldestVersionToKeep);
        } while (!success);
    }


    private void freeUnusedIfNeeded(long time) {
        int freeDelay = retentionTime / 5;
        if (time >= lastFreeUnusedChunks + freeDelay) {
            lastFreeUnusedChunks = time;
            freeUnusedChunks();
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

    private void markMetaChanged() {
        metaChanged = true;
    }

    private boolean canOverwriteChunk(Chunk c, long time) {
        if (retentionTime >= 0) {
            if (c.time + retentionTime > time) {
                return false;
            }
            if (c.unused == 0 || c.unused + retentionTime / 2 > time) {
                return false;
            }
        }
        return true;
    }

    private Set<Integer> collectReferencedChunks() {
        ChunkIdsCollector collector = new ChunkIdsCollector(meta.getId());
        Set<Long> inspectedRoots = new HashSet<>();
        long pos = lastChunk.metaRootPos;
        inspectedRoots.add(pos);
        collector.visit(pos);
        long oldestVersionToKeep = getOldestVersionToKeep();
        MVBTreeMap.RootReference rootReference = meta.getRoot();
        do {
            Page rootPage = rootReference.root;
            pos = rootPage.getPos();
            if (!rootPage.isSaved()) {
                collector.setMapId(meta.getId());
                collector.visit(rootPage);
            } else if(inspectedRoots.add(pos)) {
                collector.setMapId(meta.getId());
                collector.visit(pos);
            }

            for (Cursor<String, String> c = new Cursor<>(rootPage, "root."); c.hasNext(); ) {
                String key = c.next();
                assert key != null;
                if (!key.startsWith("root.")) {
                    break;
                }
                pos = DataUtils.parseHexLong(c.getValue());
                if (DataUtils.isPageSaved(pos) && inspectedRoots.add(pos)) {
                    // to allow for something like "root.tmp.123" to be processed
                    int mapId = DataUtils.parseHexInt(key.substring(key.lastIndexOf('.') + 1));
                    collector.setMapId(mapId);
                    collector.visit(pos);
                }
            }
        } while(rootReference.version >= oldestVersionToKeep &&
                (rootReference = rootReference.previous) != null);
        return collector.getReferenced();
    }




    void removePage(MVBTreeMap<?, ?> map, long pos, int memory) {
        if (!DataUtils.isPageSaved(pos)) {
            unsavedMemory = Math.max(0, unsavedMemory - memory);
            return;
        }
        int chunkId = DataUtils.getPageChunkId(pos);
        synchronized (freedPageSpace) {
            Chunk chunk = freedPageSpace.get(chunkId);
            if (chunk == null) {
                chunk = new Chunk(chunkId);
                freedPageSpace.put(chunkId, chunk);
            }
            chunk.maxLenLive -= DataUtils.getPageMaxLength(pos);
            chunk.pageCountLive -= 1;
        }
    }


    private Chunk getChunk(long pos) {
        Chunk c = getChunkIfFound(pos);
        if (c == null) {
            int chunkId = DataUtils.getPageChunkId(pos);
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Chunk {0} not found", chunkId);
        }
        return c;
    }


    private Chunk getChunkIfFound(long pos) {
        int chunkId = DataUtils.getPageChunkId(pos);
        Chunk c = chunks.get(chunkId);
        if (c == null) {
            checkOpen();
            if (!Thread.holdsLock(this)) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_CHUNK_NOT_FOUND,
                        "Chunk {0} no longer exists",
                        chunkId);
            }
            String s = meta.get(Chunk.getMetaKey(chunkId));
            if (s == null) {
                return null;
            }
            c = Chunk.fromString(s);
            if (c.block == Long.MAX_VALUE) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Chunk {0} is invalid", chunkId);
            }
            chunks.put(c.id, c);
        }
        return c;
    }

    private void readStoreHeader() {
        Chunk newest = null;
        boolean validStoreHeader = false;
        // 找到最新版本的chunk读取前两份区块
        ByteBuffer fileHeaderBlocks = fileStore.readFully(0, 2 * BLOCK_SIZE);
        byte[] buff = new byte[BLOCK_SIZE];
        for (int i = 0; i <= BLOCK_SIZE; i += BLOCK_SIZE) {
            fileHeaderBlocks.get(buff);
            try {
                HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
                if (m == null)
                    continue;
                int blockSize = DataUtils.readHexInt(
                        m, "blockSize", BLOCK_SIZE);
                if (blockSize != BLOCK_SIZE) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_UNSUPPORTED_FORMAT,
                            "Block size {0} is currently not supported",
                            blockSize);
                }
                long version = DataUtils.readHexLong(m, "version", 0);
                if (newest == null || version > newest.version) {
                    validStoreHeader = true;
                    storeHeader.putAll(m);
                    creationTime = DataUtils.readHexLong(m, "created", 0);
                    int chunkId = DataUtils.readHexInt(m, "chunk", 0);
                    long block = DataUtils.readHexLong(m, "block", 0);
                    Chunk test = readChunkHeaderAndFooter(block);
                    if (test != null && test.id == chunkId) {
                        newest = test;
                    }
                }
            } catch (Exception e) {
                continue;
            }
        }
        if (!validStoreHeader) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", fileStore);
        }
        long format = DataUtils.readHexLong(storeHeader, "format", 1);
        if (format > FORMAT_WRITE && !fileStore.isReadOnly()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The write format {0} is larger " +
                            "than the supported format {1}, " +
                            "and the file was not opened in read-only mode",
                    format, FORMAT_WRITE);
        }
        format = DataUtils.readHexLong(storeHeader, "formatRead", format);
        if (format > FORMAT_READ) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The read format {0} is larger " +
                            "than the supported format {1}",
                    format, FORMAT_READ);
        }
        lastStoredVersion = -1;
        chunks.clear();
        long now = System.currentTimeMillis();
        // 按四年1461天计算
        int year =  1970 + (int) (now / (1000L * 60 * 60 * 6 * 1461));
        if (year < 2014) {
            creationTime = now - fileStore.getDefaultRetentionTime();
        } else if (now < creationTime) {
            creationTime = now;
            storeHeader.put("created", creationTime);
        }
        Chunk test = readChunkFooter(fileStore.size());
        if (test != null) {
            test = readChunkHeaderAndFooter(test.block);
            if (test != null) {
                if (newest == null || test.version > newest.version) {
                    newest = test;
                }
            }
        }
        if (newest == null) {
            // no chunk
            return;
        }
        // 通过header和footer来遍历chunk chain这个文件化的双向链表
        while (true) {
            if (newest.next == 0 ||
                    newest.next >= fileStore.size() / BLOCK_SIZE) {
                break;
            }
            test = readChunkHeaderAndFooter(newest.next);
            if (test == null || test.id <= newest.id) {
                break;
            }
            newest = test;
        }
        do {
            setLastChunk(newest);
            loadChunkMeta();
            fileStore.clear();
            for (Chunk c : chunks.values()) {
                long start = c.block * BLOCK_SIZE;
                int length = c.len * BLOCK_SIZE;
                fileStore.markUsed(start, length);
            }
            assert fileStore.getFileLengthInUse() == measureFileLengthInUse() :
                    fileStore.getFileLengthInUse() + " != " + measureFileLengthInUse();
        } while((newest = verifyLastChunks()) != null);

        setWriteVersion(currentVersion);
        if (lastStoredVersion == INITIAL_VERSION) {
            lastStoredVersion = currentVersion - 1;
        }
        assert fileStore.getFileLengthInUse() == measureFileLengthInUse() :
                fileStore.getFileLengthInUse() + " != " + measureFileLengthInUse();
    }

    private Chunk verifyLastChunks() {
        assert lastChunk == null || chunks.containsKey(lastChunk.id) : lastChunk;
        BitSet validIds = new BitSet();
        Queue<Chunk> queue = new PriorityQueue<>(chunks.size(), new Comparator<Chunk>() {
            @Override
            public int compare(Chunk one, Chunk two) {
                return Integer.compare(one.id, two.id);
            }
        });
        queue.addAll(chunks.values());
        int newestValidChunk = -1;
        Chunk c;
        while((c = queue.poll()) != null) {
            Chunk test = readChunkHeaderAndFooter(c.block);
            if (test == null || test.id != c.id) {
                continue;
            }
            validIds.set(c.id);

            try {
                MVBTreeMap<String, String> oldMeta = meta.openReadOnly(c.metaRootPos, c.version);
                boolean valid = true;
                for(Iterator<String> iter = oldMeta.keyIterator("chunk."); valid && iter.hasNext(); ) {
                    String s = iter.next();
                    if (!s.startsWith("chunk.")) {
                        break;
                    }
                    s = oldMeta.get(s);
                    valid = validIds.get(Chunk.fromString(s).id);
                }
                if (valid) {
                    newestValidChunk = c.id;
                }
            } catch (Exception ignore) {/**/}
        }

        Chunk newest = chunks.get(newestValidChunk);
        if (newest != lastChunk) {
            if (newest == null) {
                rollbackTo(0);
            } else {
                rollbackTo(newest.version);
                return newest;
            }
        }
        return  null;
    }

    /**
     * 回滚至当前版本的初始状态，忽略所有未提交的更改
     */
    public void rollback() {
        rollbackTo(currentVersion);
    }

    /**
     * 回归至某一版本。回滚后，所有目标版本之后的更改全部废弃。所有之后创建的版本控制Map全部关闭，
     * 回滚至0意味者删除此Chunk相关的所有数据
     * @param version
     */
    public synchronized void rollbackTo(long version) {
        checkOpen();
        if (version == 0) {
            for (MVBTreeMap<?, ?> m : maps.values()) {
                m.close();
            }
            meta.setInitialRoot(meta.createEmptyLeaf(), INITIAL_VERSION);
            chunks.clear();
            if (fileStore != null) {
                fileStore.clear();
            }
            maps.clear();
            lastChunk = null;
            synchronized (freedPageSpace) {
                freedPageSpace.clear();
            }
            versions.clear();
            currentVersion = version;
            setWriteVersion(version);
            metaChanged =  false;
            lastStoredVersion = INITIAL_VERSION;
            return;
        }
        DataUtils.checkArgument(
                isKnownVersion(version),
                "Unknown version {0}", version);
        for (MVBTreeMap<?, ?> m : maps.values()) {
            m.rollbackTo(version);
        }
        TxCounter txCounter;
        while ((txCounter = versions.peekLast()) != null && txCounter.version >= version) {
            versions.removeLast();
        }
        currentTxCounter = new TxCounter(version);

        meta.rollbackTo(version);
        metaChanged = false;
        boolean loadFromFile = false;
        // 查询需要移除的chunk和需要获取的最近版本对应的chunk
        ArrayList<Integer> remove = new ArrayList<>();
        Chunk keep = null;
        for (Chunk c : chunks.values()) {
            if (c.version > version) {
                remove.add(c.id);
            } else if (keep == null || keep.id < c.id) {
                keep = c;
            }
        }
        if (!remove.isEmpty()) {
            Collections.sort(remove, Collections.reverseOrder());
            loadFromFile = true;
            for (int id : remove) {
                Chunk c = chunks.remove(id);
                long start = c.block * BLOCK_SIZE;
                int length = c.len * BLOCK_SIZE;
                //向bitset注册被释放的空间
                fileStore.free(start, length);
                assert fileStore.getFileLengthInUse() == measureFileLengthInUse() :
                        fileStore.getFileLengthInUse() + " != " + measureFileLengthInUse();
                WriteBuffer buff = getWriteBuffer();
                buff.limit(length);
                Arrays.fill(buff.getBuffer().array(), (byte) 0);
                write(start, buff.getBuffer());
                releaseWriteBuffer(buff);
                sync();
            }
            lastChunk = keep;
            writeStoreHeader();
            readStoreHeader();
        }
        for (MVBTreeMap<?, ?> m : new ArrayList<>(maps.values())) {
            int id = m.getId();
            if (m.getCreateVersion() >= version) {
                m.close();
                maps.remove(id);
            } else {
                if (loadFromFile) {
                    m.setRootPos(getRootPos(meta, id), version);
                } else {
                    m.rollbackRoot(version);
                }
            }
        }
        currentVersion = version;
        if (lastStoredVersion == INITIAL_VERSION) {
            lastStoredVersion = currentVersion - 1;
        }
    }

    private void releaseWriteBuffer(WriteBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }
    }


    /**
     * 获取WriteBuffer，调用此方法的caller必须在store时加同步锁
     * @return
     */
    private WriteBuffer getWriteBuffer() {
        WriteBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = new WriteBuffer();
        }
        return buff;
    }


    private void loadChunkMeta() {
        for (Iterator<String> it = meta.keyIterator("chunk."); it.hasNext();) {
            String s = it.next();
            if (!s.startsWith("chunk.")) {
                break;
            }
            s = meta.get(s);
            Chunk c = Chunk.fromString(s);
            if (chunks.putIfAbsent(c.id, c) == null) {
                if (c.block == Long.MAX_VALUE) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_FILE_CORRUPT,
                            "Chunk {0} is invalid", c.id);
                }
            }
        }
    }

    private boolean isKnownVersion(long version) {
        if (version > currentVersion || version < 0) {
            return false;
        }
        if (version == currentVersion || chunks.size() == 0) {
            return true;
        }
        Chunk c = getChunkForVersion(version);
        if (c == null) {
            return false;
        }
        MVBTreeMap<String, String> oldMeta = getMetaMap(version);
        if (oldMeta == null) {
            return false;
        }
        try {
            for (Iterator<String> it = oldMeta.keyIterator("chunk.");
                 it.hasNext();) {
                String chunkKey = it.next();
                if (!chunkKey.startsWith("chunk.")) {
                    break;
                }
                if (!meta.containsKey(chunkKey)) {
                    String s = oldMeta.get(chunkKey);
                    Chunk c2 = Chunk.fromString(s);
                    Chunk test = readChunkHeaderAndFooter(c2.block);
                    if (test == null || test.id != c2.id) {
                        return false;
                    }
                    chunks.put(c2.id, c2);
                }
            }
        } catch (IllegalStateException e) {
            return false;
        }
        return true;
    }



    private void setLastChunk(Chunk last) {
        lastChunk = last;
        if (last == null) {
            // no valid chunk
            lastMapId = 0;
            currentVersion = 0;
            meta.setRootPos(0, -1);
        } else {
            lastMapId = last.mapId;
            currentVersion = last.version;
            chunks.put(last.id, last);
            meta.setRootPos(last.metaRootPos, -1);
        }
        setWriteVersion(currentVersion);
    }

    private void setWriteVersion(long version) {
        for (Iterator<MVBTreeMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVBTreeMap<?, ?> map = iter.next();
            if (map.setWriteVersion(version) == null) {
                assert map.isClosed();
                assert map.getVersion() < getOldestVersionToKeep();
                meta.remove(MVBTreeMap.getMapRootKey(map.getId()));
                markMetaChanged();
                iter.remove();
            }
        }
        meta.setWriteVersion(version);
        onVersionChange(version);
    }


    private Chunk readChunkHeaderAndFooter(long block) {
        Chunk header;
        try {
            header = readChunkHeader(block);
        } catch (Exception e) {
            // invalid chunk header: ignore, but stop
            return null;
        }
        if (header == null) {
            return null;
        }
        Chunk footer = readChunkFooter((block + header.len) * BLOCK_SIZE);
        if (footer == null || footer.id != header.id) {
            return null;
        }
        return header;
    }

    private Chunk readChunkHeader(long block) {
        long p = block * BLOCK_SIZE;
        ByteBuffer buff = fileStore.readFully(p, Chunk.MAX_HEADER_LENGTH);
        return Chunk.readChunkHeader(buff, p);
    }


    private Chunk readChunkFooter(long end) {
        try {
            // 读取chunk的最后一个区块
            ByteBuffer lastBlock = fileStore.readFully(
                    end - Chunk.FOOTER_LENGTH, Chunk.FOOTER_LENGTH);
            byte[] buff = new byte[Chunk.FOOTER_LENGTH];
            lastBlock.get(buff);
            HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
            if (m != null) {
                int chunk = DataUtils.readHexInt(m, "chunk", 0);
                Chunk c = new Chunk(chunk);
                c.version = DataUtils.readHexLong(m, "version", 0);
                c.block = DataUtils.readHexLong(m, "block", 0);
                return c;
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    Page readPage(MVBTreeMap<?, ?> map, long pos) {
        if (pos == 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        }
        Page p = cache == null ? null : cache.get(pos);
        if (p == null) {
            Chunk c = getChunk(pos);
            long filePos = c.block * BLOCK_SIZE;
            filePos += DataUtils.getPageOffset(pos);
            if (filePos < 0) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Negative position {0}", filePos);
            }
            long maxPos = (c.block + c.len) * BLOCK_SIZE;
            p = Page.read(fileStore, pos, map, filePos, maxPos);
            cachePage(p);
        }
        return p;
    }


    void cachePage(Page page) {
        if (cache != null) {
            cache.put(page.getPos(), page, page.getMemory());
        }
    }


    long getOldestVersionToKeep() {
        long v = currentVersion;
        if (fileStore == null) {
            return v - versionsToKeep;
        }
        long storeVersion = currentStoreVersion;
        if (storeVersion > -1) {
            v = Math.min(v, storeVersion);
        }
        return v;
    }

    Compressor getCompressorFast() {
        if (compressorFast == null) {
            compressorFast = new CompressLZF();
        }
        return compressorFast;
    }

    Compressor getCompressorHigh() {
        if (compressorHigh == null) {
            compressorHigh = new CompressDeflate();
        }
        return compressorHigh;
    }

    /**
     * 后台定时commit，策略和mongodb一致(mongo会fork sub-process)，而不同于lucene默认的内存阀值策略
     */
    void writeInBackground() {

        try {
            if (closed) {
                return;
            }

            long time = getTimeSinceCreation();
            if (time <= lastCommitTime + autoCommitDelay) {
                return;
            }
            tryCommit();
            if (autoCompactFillRate > 0) {
                boolean fileOps;
                long fileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
                if (autoCompactLastFileOpCount != fileOpCount) {
                    fileOps = true;
                } else {
                    fileOps = false;
                }
                int targetFillRate = fileOps ? autoCompactFillRate / 3 : autoCompactFillRate;
                compact(targetFillRate, autoCommitMemory);
                autoCompactLastFileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
            }
        } catch (Throwable e) {
            handleException(e);
        }


    }

    /**
     * 将full chunk的数据分配到low fill rate chunk中
     * @param targetFillRate
     * @param write
     * @return
     */
    public boolean compact(int targetFillRate, int write) {
        if (!reuseSpace) {
            return false;
        }
        synchronized (compactSync) {
            checkOpen();
            ArrayList<Chunk> old;
            synchronized (this) {
                old = compactGetOldChunks(targetFillRate, write);
            }
            if (old == null || old.isEmpty()) {
                return false;
            }
            compactRewrite(old);
            return true;
        }
    }

    private void compactRewrite(Iterable<Chunk> old) {
        HashSet<Integer> set = new HashSet<>();
        for (Chunk c : old) {
            set.add(c.id);
        }
        for (MVBTreeMap<?, ?> m : maps.values()) {
            @SuppressWarnings("unchecked")
            MVBTreeMap<Object, Object> map = (MVBTreeMap<Object, Object>) m;
            if (!map.isClosed()) {
                map.rewrite(set);
            }
        }
        meta.rewrite(set);
        freeUnusedChunks();
        commit();
    }



    private ArrayList<Chunk> compactGetOldChunks(int targetFillRate, int write) {

        if (lastChunk == null) {
            return null;
        }

        long maxLengthSum = 0;
        long maxLengthLiveSum = 0;

        long time = getTimeSinceCreation();

        for (Chunk c : chunks.values()) {
            //不回收年轻代
            if (c.time + retentionTime > time) {
                continue;
            }
            maxLengthSum += c.maxLen;
            maxLengthLiveSum += c.maxLenLive;
        }

        if (maxLengthLiveSum < 0) {
            return null;
        }

        if (maxLengthSum <= 0) {
            //避免分母为0
            maxLengthLiveSum =1;
        }

        int fillRate = (int) (100 * maxLengthLiveSum / maxLengthSum);
        if (fillRate >= targetFillRate) {
            return null;
        }

        //老年代
        ArrayList<Chunk> old = New.arrayList();

        Chunk last = chunks.get(lastChunk.id);

        for (Chunk c : chunks.values()) {
            if (c.time + retentionTime > time) {
                continue;
            }
            long age = last.version - c.version + 1;
            c.collectPriority = (int) (c.getFillRate() * 1000 / age);
            old.add(c);
        }
        if (old.isEmpty()) {
            return null;
        }

        Collections.sort(old, new Comparator<Chunk>() {
            @Override
            public int compare(Chunk o1, Chunk o2) {
                int comp = Integer.compare(o1.collectPriority,
                        o2.collectPriority);
                if (comp == 0) {
                    comp = Long.compare(o1.maxLenLive,
                            o2.maxLenLive);
                }
                return comp;
            }
        });

        long written = 0;
        int chunkCount = 0;

        Chunk move = null;

        for (Chunk c : old) {
            if (move != null) {
                if (c.collectPriority > 0 && written > write) {
                    break;
                }
            }
            written += c.maxLenLive;
            chunkCount++;
            move = c;
        }

        if (chunkCount < 1) {
            return null;
        }

        boolean remove = false;
        for (Iterator<Chunk> it = old.iterator(); it.hasNext();) {
            Chunk c = it.next();
            if (move == c) {
                remove = true;
            } else if (remove) {
                it.remove();
            }
        }
        return old;

    }


    long getRootPos(int mapId, long version) {
        MVBTreeMap<String, String> oldMeta = getMetaMap(version);
        return getRootPos(oldMeta, mapId);
    }

    private static long getRootPos(MVBTreeMap<String, String> map, int mapId) {
        String root = map.get(MVBTreeMap.getMapRootKey(mapId));
        return root == null ? 0 : DataUtils.parseHexLong(root);
    }

    private MVBTreeMap<String, String> getMetaMap(long version) {
        Chunk c = getChunkForVersion(version);
        DataUtils.checkArgument(c != null, "Unknown version {0}", version);
        c = readChunkHeader(c.block);
        MVBTreeMap<String, String> oldMeta = meta.openReadOnly(c.metaRootPos, version);
        return oldMeta;
    }

    private Chunk getChunkForVersion(long version) {
        Chunk newest = null;
        for (Chunk c : chunks.values()) {
            if (c.version <= version) {
                if (newest == null || c.id > newest.id) {
                    newest = c;
                }
            }
        }
        return newest;
    }

    int getCompressionLevel() {
        return compressionLevel;
    }


    final class ChunkIdsCollector {

        private final Set<Integer>      referenced = new HashSet<>();
        private final ChunkIdsCollector parent;
        private       ChunkIdsCollector child;
        private       int               mapId;

        ChunkIdsCollector(int mapId) {
            this.parent = null;
            this.mapId = mapId;
        }

        private ChunkIdsCollector(ChunkIdsCollector parent) {
            this.parent = parent;
            this.mapId = parent.mapId;
        }

        public int getMapId() {
            return mapId;
        }

        public void setMapId(int mapId) {
            this.mapId = mapId;
            if (child != null) {
                child.setMapId(mapId);
            }
        }

        public Set<Integer> getReferenced() {
            return referenced;
        }

        public void visit(Page page) {
            long pos = page.getPos();
            if (DataUtils.isPageSaved(pos)) {
                register(DataUtils.getPageChunkId(pos));
            }
            int count = page.map.getChildPageCount(page);
            if (count > 0) {
                ChunkIdsCollector childCollector = getChild();
                for (int i = 0; i < count; i++) {
                    Page childPage = page.getChildPageIfLoaded(i);
                    if (childPage != null) {
                        childCollector.visit(childPage);
                    } else {
                        childCollector.visit(page.getChildPagePos(i));
                    }
                }
                // chunk缓存
                if (DataUtils.isPageSaved(pos) && cacheChunkRef != null) {
                    int[] chunkIds = childCollector.getChunkIds();
                    cacheChunkRef.put(pos, chunkIds, Constants.MEMORY_ARRAY + 4 * chunkIds.length);
                }
            }
        }

        public void visit(long pos) {
            if (!DataUtils.isPageSaved(pos)) {
                return;
            }
            register(DataUtils.getPageChunkId(pos));
            if (DataUtils.getPageType(pos) != DataUtils.PAGE_TYPE_LEAF) {
                int chunkIds[];
                if (cacheChunkRef != null && (chunkIds = cacheChunkRef.get(pos)) != null) {
                    // 先从缓存中取chunk
                    for (int chunkId : chunkIds) {
                        register(chunkId);
                    }
                } else {
                    ChunkIdsCollector childCollector = getChild();
                    Page page;
                    if (cache != null && (page = cache.get(pos)) != null) {
                        // 从缓存中取page
                        childCollector.visit(page);
                    } else {
                        // 缓存中没有page，read page
                        Chunk chunk = getChunk(pos);
                        long filePos = chunk.block * BLOCK_SIZE;
                        filePos += DataUtils.getPageOffset(pos);
                        if (filePos < 0) {
                            throw DataUtils.newIllegalStateException(
                                    DataUtils.ERROR_FILE_CORRUPT,
                                    "Negative position {0}; p={1}, c={2}", filePos, pos, chunk.toString());
                        }
                        long maxPos = (chunk.block + chunk.len) * BLOCK_SIZE;
                        Page.readChildrenPositions(fileStore, pos, filePos, maxPos, childCollector);
                    }
                    if (cacheChunkRef != null) {
                        chunkIds = childCollector.getChunkIds();
                        cacheChunkRef.put(pos, chunkIds, Constants.MEMORY_ARRAY + 4 * chunkIds.length);
                    }
                }
            }
        }

        private ChunkIdsCollector getChild() {
            if (child == null) {
                child = new ChunkIdsCollector(this);
            } else {
                child.referenced.clear();
            }
            return child;
        }

        private void register(int chunkId) {
            if (referenced.add(chunkId) && parent != null) {
                parent.register(chunkId);
            }
        }

        private int[] getChunkIds() {
            int chunkIds[] = new int[referenced.size()];
            int index = 0;
            for (int chunkId : referenced) {
                chunkIds[index++] = chunkId;
            }
            return chunkIds;
        }
    }


    public static final class TxCounter {
        public final long version;
        public final AtomicInteger counter = new AtomicInteger();

        TxCounter(long version) {
            this.version = version;
        }

        @Override
        public String toString() {
            return "v=" + version + " / cnt=" + counter;
        }
    }


    private static class BackgroundWriterThread extends Thread {

        /**
         * 同步锁
         */
        public final Object sync = new Object();

        private final BTreeForest bTree;

        private final int sleep;

        BackgroundWriterThread(BTreeForest bTree, int sleep, String fileStoreName) {
            super("BTree background writer " + fileStoreName);
            this.bTree = bTree;
            this.sleep = sleep;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (true) {
                Thread t = bTree.backgroundWriterThread;
                if (t == null) {
                    break;
                }
                synchronized (sync) {
                    try {
                        sync.wait(sleep);
                    } catch (InterruptedException e) {
                        continue;
                    }
                }
                bTree.writeInBackground();
            }
        }

    }


    public <K, V> MVBTreeMap<K, V> openMap(String name) {
        return openMap(name, new MVBTreeMap.Builder<K, V>());
    }

    public synchronized <M extends MVBTreeMap<K, V>, K, V> M openMap(
            String name, MVBTreeMap.MapBuilder<M, K, V> builder) {
        int id = getMapId(name);
        M map;
        if (id >= 0) {
            map = openMap(id, builder);
        } else {
            HashMap<String, Object> c = new HashMap<>();
            id = ++lastMapId;
            c.put("id", id);
            c.put("createVersion", currentVersion);
            map = builder.create(this, c);
            map.init();
            String x = Integer.toHexString(id);
            meta.put(MVBTreeMap.getMapKey(id), map.asString(name));
            meta.put("name." + name, x);
            map.setRootPos(0, lastStoredVersion);
            markMetaChanged();
            @SuppressWarnings("unchecked")
            M existingMap = (M)maps.putIfAbsent(id, map);
            if(existingMap != null) {
                map = existingMap;
            }
        }
        return map;
    }

    private int getMapId(String name) {
        String m = meta.get("name." + name);
        return m == null ? -1 : DataUtils.parseHexInt(m);
    }

    public synchronized <M extends MVBTreeMap<K, V>, K, V> M openMap(int id,
                                                                     MVBTreeMap.MapBuilder<M, K, V> builder) {
        @SuppressWarnings("unchecked")
        M map = (M) getMap(id);
        if (map == null) {
            String configAsString = meta.get(MVBTreeMap.getMapKey(id));
            if(configAsString != null) {
                HashMap<String, Object> config =
                        new HashMap<String, Object>(DataUtils.parseMap(configAsString));
                config.put("id", id);
                map = builder.create(this, config);
                map.init();
                long root = getRootPos(meta, id);
                map.setRootPos(root, lastStoredVersion);
                maps.put(id, map);
            }
        }
        return map;
    }

    public <K, V> MVBTreeMap<K,V> getMap(int id) {
        checkOpen();
        @SuppressWarnings("unchecked")
        MVBTreeMap<K, V> map = (MVBTreeMap<K, V>) maps.get(id);
        return map;
    }


    /**
     * 嵌入式应用的构造模式
     */
    public static final class Builder {

        private final HashMap<String, Object> config;

        private Builder(HashMap<String, Object> config) {
            this.config = config;
        }

        public Builder() {
            config = new HashMap<>();
        }

        private Builder set(String key, Object value) {
            config.put(key, value);
            return this;
        }

        /**
         * 关闭autoCommit，将不会启动持续进行持久化任务的Daemon线程
         *
         * @return this
         */
        public Builder autoCommitDisabled() {
            set("autoCommitBufferSize", 0);
            return set("autoCommitDelay", 0);
        }

        /**
         * 设置内存更改量触发持久化的阀值
         *
         * @param kb the write buffer size, in kilobytes
         * @return this
         */
        public Builder autoCommitBufferSize(int kb) {
            return set("autoCommitBufferSize", kb);
        }

        /**
         * 如果一个chunk的填充率低于目标填充率，则重写chunk。如果chunks之间的empty space的百分比
         * 大于目标填充率，则remove文件的最后一个chunk，直到小于目标填充率
         *
         * @param percent the target fill rate
         * @return this
         */
        public Builder autoCompactFillRate(int percent) {
            return set("autoCompactFillRate", percent);
        }

        /**
         * 打开下列filename的文件，如果文件不存在则创建一个
         *
         * @param fileName the file name
         * @return this
         */
        public Builder fileName(String fileName) {
            return set("fileName", fileName);
        }

        /**
         * 通过一个char array维护的password来加密/解密数据
         *
         * @param password the password
         * @return this
         */
        public Builder encryptionKey(char[] password) {
            return set("encryptionKey", password);
        }

        /**
         * 以只读模式打开文件，此时给文件加上一个share lock来保证不以write模式访问文件。
         * 如果不以这种模式打开文件，将给文件加上excessive lock
         *
         * @return this
         */
        public Builder readOnly() {
            return set("readOnly", 1);
        }

        /**
         * 设置缓存大小(MB),默认为16MB
         *
         * @param mb the cache size in megabytes
         * @return this
         */
        public Builder cacheSize(int mb) {
            return set("cacheSize", mb);
        }

        /**
         * 设置读read cache的并发段数
         *
         * @param concurrency the cache concurrency
         * @return this
         */
        public Builder cacheConcurrency(int concurrency) {
            return set("cacheConcurrency", concurrency);
        }

        /**
         * 使用LZF算法压缩数据，可以节约一半左右的磁盘空间，但是会拖慢读写速度
         *
         * @return this
         */
        public Builder compress() {
            return set("compress", 1);
        }

        /**
         * 使用默认算法压缩，对读写速度的影响较小
         *
         * @return this
         */
        public Builder compressHigh() {
            return set("compress", 2);
        }

        /**
         * 设置一个page node最多能持有的容量，内存中是4kb，持久化最多16kb，超过则进行分裂
         *
         * @param pageSplitSize the page size
         * @return this
         */
        public Builder pageSplitSize(int pageSplitSize) {
            return set("pageSplitSize", pageSplitSize);
        }

        /**
         * Set the listener to be used for exceptions that occur when writing in
         * the background thread.
         *
         * @param exceptionHandler the handler
         * @return this
         */
        public Builder backgroundExceptionHandler(
                Thread.UncaughtExceptionHandler exceptionHandler) {
            return set("backgroundExceptionHandler", exceptionHandler);
        }

        /**
         * 使用特定的fileStore
         *
         * @param store the file store
         * @return this
         */
        public Builder fileStore(FileStore store) {
            return set("fileStore", store);
        }

        /**
         * 打开一个森林实例
         *
         * @return the opened store
         */
        public BTreeForest open() {
            return new BTreeForest(config);
        }

        @Override
        public String toString() {
            return DataUtils.appendMap(new StringBuilder(), config).toString();
        }

        /**
         * 从文本编码读取一个config实例
         *
         * @param s the string representation
         * @return the builder
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public static Builder fromString(String s) {
            return new Builder((HashMap) DataUtils.parseMap(s));
        }
    }



}
