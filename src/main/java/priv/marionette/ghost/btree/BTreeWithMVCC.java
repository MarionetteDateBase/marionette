package priv.marionette.ghost.btree;

import priv.marionette.cache.LIRSCache;
import priv.marionette.compress.CompressDeflate;
import priv.marionette.compress.CompressLZF;
import priv.marionette.compress.Compressor;
import priv.marionette.ghost.*;
import priv.marionette.ghost.type.StringDataType;
import priv.marionette.tools.DataUtils;
import priv.marionette.tools.New;

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

            // autoCommit慢启动
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
        // start the background thread if needed
        if (millis > 0) {
            int sleep = Math.max(1, millis / 10);
            BackgroundWriterThread t =
                    new BackgroundWriterThread(this, sleep,
                            fileStore.toString());
            t.start();
            backgroundWriterThread = t;
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
            for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
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

    void beforeWrite(MVMap<?, ?> map) {
        if (saveNeeded) {
            if (map == meta) {
                //避免btree和concurrent control操作类同时持有锁从而引起死锁
                return;
            }
            saveNeeded = false;
            if (unsavedMemory > autoCommitMemory && autoCommitMemory > 0) {
                commitAndSave();
            }
        }
    }

    public int getPageSplitSize() {
        return pageSplitSize;
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


    void removePage(MVMap<?, ?> map, long pos, int memory) {
        // 如果这个page还没有被持久化，那么作为旧版本临时保存
        if (pos == 0) {
            unsavedMemory = Math.max(0, unsavedMemory - memory);
            return;
        }

        if (cache != null) {
            if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
                // 只要不是叶子节点就继续暂时保存至cache，因为要让node参与垃圾回收
                cache.remove(pos);
            }
        }

        Chunk c = getChunk(pos);
        long version = currentVersion;
        if (map == meta && currentStoreVersion >= 0) {
            if (Thread.currentThread() == currentStoreThread) {
                // 如果page在删除的同时，其所属的chunk的对应持久化操作线程在执行持久化，
                // 那么将被释放的page对应的chunk版本保存下来，以保证chunk的旧版本可用性
                version = currentStoreVersion;
            }
        }
        registerFreePage(version, c.id,
                DataUtils.getPageMaxLength(pos), 1);
    }

    private void registerFreePage(long version, int chunkId,
                                  long maxLengthLive, int pageCount) {
        ConcurrentHashMap<Integer, Chunk> freed = freedPageSpace.get(version);
        if (freed == null) {
            freed = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, Chunk> f2 = freedPageSpace.putIfAbsent(version,
                    freed);
            if (f2 != null) {
                freed = f2;
            }
        }
        synchronized (freed) {
            Chunk chunk = freed.get(chunkId);
            if (chunk == null) {
                chunk = new Chunk(chunkId);
                Chunk chunk2 = freed.putIfAbsent(chunkId, chunk);
                if (chunk2 != null) {
                    chunk = chunk2;
                }
            }
            chunk.maxLenLive -= maxLengthLive;
            chunk.pageCountLive -= pageCount;
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

    private synchronized void readStoreHeader() {
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
        setLastChunk(newest);
        loadChunkMeta();
        // 检查chunk的header和footer来去除由于断电等原因造成的错误写入版本
        verifyLastChunks();
        for (Chunk c : chunks.values()) {
            if (c.pageCountLive == 0) {
                registerFreePage(currentVersion, c.id, 0, 0);
            }
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            fileStore.markUsed(start, length);
        }
    }

    private void verifyLastChunks() {
        long time = getTimeSinceCreation();
        ArrayList<Integer> ids = new ArrayList<>(chunks.keySet());
        Collections.sort(ids);
        int newestValidChunk = -1;
        Chunk old = null;
        for (Integer chunkId : ids) {
            Chunk c = chunks.get(chunkId);
            if (old != null && c.time < old.time) {
                break;
            }
            old = c;
            if (c.time + retentionTime < time) {
                newestValidChunk = c.id;
                continue;
            }
            Chunk test = readChunkHeaderAndFooter(c.block);
            if (test == null || test.id != c.id) {
                break;
            }
            newestValidChunk = chunkId;
        }
        Chunk newest = chunks.get(newestValidChunk);
        if (newest != lastChunk) {
            //回滚至正确的情况下最新的数据版本
            rollbackTo(newest == null ? 0 : newest.version);
        }
    }

    /**
     * 回归至某一版本。回滚后，所有目标版本之后的更改全部废弃。所有之后创建的版本控制Map全部关闭，
     * 回滚至0意味者删除此Chunk相关的所有数据
     * @param version
     */
    public synchronized void rollbackTo(long version) {
        checkOpen();
        if (version == 0) {
            // special case: remove all data
            for (MVMap<?, ?> m : maps.values()) {
                m.close();
            }
            meta.clear();
            chunks.clear();
            if (fileStore != null) {
                fileStore.clear();
            }
            maps.clear();
            freedPageSpace.clear();
            currentVersion = version;
            setWriteVersion(version);
            metaChanged = false;
            return;
        }
        DataUtils.checkArgument(
                isKnownVersion(version),
                "Unknown version {0}", version);
        for (MVMap<?, ?> m : maps.values()) {
            m.rollbackTo(version);
        }
        for (long v = currentVersion; v >= version; v--) {
            if (freedPageSpace.size() == 0) {
                break;
            }
            freedPageSpace.remove(v);
        }
        meta.rollbackTo(version);
        metaChanged = false;
        boolean loadFromFile = false;
        // find out which chunks to remove,
        // and which is the newest chunk to keep
        // (the chunk list can have gaps)
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
            // remove the youngest first, so we don't create gaps
            // (in case we remove many chunks)
            Collections.sort(remove, Collections.reverseOrder());
            revertTemp(version);
            loadFromFile = true;
            for (int id : remove) {
                Chunk c = chunks.remove(id);
                long start = c.block * BLOCK_SIZE;
                int length = c.len * BLOCK_SIZE;
                fileStore.free(start, length);
                // overwrite the chunk,
                // so it is not be used later on
                WriteBuffer buff = getWriteBuffer();
                buff.limit(length);
                // buff.clear() does not set the data
                Arrays.fill(buff.getBuffer().array(), (byte) 0);
                write(start, buff.getBuffer());
                releaseWriteBuffer(buff);
                // only really needed if we remove many chunks, when writes are
                // re-ordered - but we do it always, because rollback is not
                // performance critical
                sync();
            }
            lastChunk = keep;
            writeStoreHeader();
            readStoreHeader();
        }
        for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
            int id = m.getId();
            if (m.getCreateVersion() >= version) {
                m.close();
                maps.remove(id);
            } else {
                if (loadFromFile) {
                    m.setRootPos(getRootPos(meta, id), -1);
                }
            }
        }
        // rollback might have rolled back the stored chunk metadata as well
        if (lastChunk != null) {
            for (Chunk c : chunks.values()) {
                meta.put(Chunk.getMetaKey(c.id), c.asString());
            }
        }
        currentVersion = version;
        setWriteVersion(version);
    }



    private void loadChunkMeta() {
        // load the chunk metadata: we can load in any order,
        // because loading chunk metadata might recursively load another chunk
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
        for (MVMap<?, ?> map : maps.values()) {
            map.setWriteVersion(version);
        }
        MVMap<String, String> m = meta;
        if (m == null) {
            checkOpen();
        }
        m.setWriteVersion(version);
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

    Page readPage(MVMap<?, ?> map, long pos) {
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
            cachePage(pos, p, p.getMemory());
        }
        return p;
    }


    void cachePage(long pos, Page page, int memory) {
        if (cache != null) {
            cache.put(pos, page, memory);
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
            long time = getTimeSinceCreation();

            if (time <= lastCommitTime + autoCommitDelay) {
                return;
            }

            if (hasUnsavedChanges()) {
                try {
                    commitAndSave();
                } catch (Throwable e) {
                    handleException(e);
                    return;
                }
            }

            if (autoCompactFillRate > 0) {
                boolean fileOps;
                long fileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
                if (autoCompactLastFileOpCount != fileOpCount) {
                    fileOps = true;
                } else {
                    fileOps = false;
                }
                // 如果有任何文件操作，执行一个较低的fill rate
                int fillRate = fileOps ? autoCompactFillRate / 3 : autoCompactFillRate;
                compact(fillRate, autoCommitMemory);
                autoCompactLastFileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
            }

        }catch (Throwable e){
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

    private void compactRewrite(ArrayList<Chunk> old) {
        HashSet<Integer> set = new HashSet<>();
        for (Chunk c : old) {
            set.add(c.id);
        }
        for (MVMap<?, ?> m : maps.values()) {
            @SuppressWarnings("unchecked")
            MVMap<Object, Object> map = (MVMap<Object, Object>) m;
            if (!map.rewrite(set)) {
                return;
            }
        }
        if (!meta.rewrite(set)) {
            return;
        }
        freeUnusedChunks();
        commitAndSave();
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

    /**
     * 打开一个磁盘上的旧版本数据
     * @param version
     * @param mapId
     * @param template
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    <T extends MVMap<?, ?>> T openMapVersion(long version, int mapId,
                                             MVMap<?, ?> template) {
        MVMap<String, String> oldMeta = getMetaMap(version);
        long rootPos = getRootPos(oldMeta, mapId);
        MVMap<?, ?> m = template.openReadOnly();
        m.setRootPos(rootPos, version);
        return (T) m;
    }

    private static long getRootPos(MVMap<String, String> map, int mapId) {
        String root = map.get(MVMap.getMapRootKey(mapId));
        return root == null ? 0 : DataUtils.parseHexLong(root);
    }

    private MVMap<String, String> getMetaMap(long version) {
        Chunk c = getChunkForVersion(version);
        DataUtils.checkArgument(c != null, "Unknown version {0}", version);
        c = readChunkHeader(c.block);
        MVMap<String, String> oldMeta = meta.openReadOnly();
        oldMeta.setRootPos(c.metaRootPos, version);
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





    private static class BackgroundWriterThread extends Thread {

        /**
         * 同步锁
         */
        public final Object sync = new Object();

        private final BTreeWithMVCC bTree;

        private final int sleep;

        BackgroundWriterThread(BTreeWithMVCC bTree, int sleep, String fileStoreName) {
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



}
