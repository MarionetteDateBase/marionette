package priv.marionette.ghost.btree;

import priv.marionette.cache.LIRSCache;
import priv.marionette.compress.CompressDeflate;
import priv.marionette.compress.CompressLZF;
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
        // calculate the year (doesn't have to be exact;
        // we assume 365.25 days per year, * 4 = 1461)
        int year =  1970 + (int) (now / (1000L * 60 * 60 * 6 * 1461));
        if (year < 2014) {
            // if the year is before 2014,
            // we assume the system doesn't have a real-time clock,
            // and we set the creationTime to the past, so that
            // existing chunks are overwritten
            creationTime = now - fileStore.getDefaultRetentionTime();
        } else if (now < creationTime) {
            // the system time was set to the past:
            // we change the creation time
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
        // read the chunk header and footer,
        // and follow the chain of next chunks
        while (true) {
            if (newest.next == 0 ||
                    newest.next >= fileStore.size() / BLOCK_SIZE) {
                // no (valid) next
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
        // read all chunk headers and footers within the retention time,
        // to detect unwritten data after a power failure
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




    private static class BackgroundWriterThread extends Thread {
        public final Object sync = new Object();

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





}
