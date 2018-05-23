package priv.marionette.ghost.kv;

import priv.marionette.ghost.type.DataType;
import priv.marionette.ghost.type.ObjectDataType;
import priv.marionette.ghost.type.StringDataType;
import priv.marionette.tools.DataUtils;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * B树节点多版本并发控制持久化操作类
 *
 * <p>
 * 当进行read操作时，保证和其他类型操作同时进行的情况下不产生并发安全问题
 *
 * <p>
 * 当进行write操作时，如果数据在磁盘上，先从磁盘中读取相关数据至内存，
 * 然后在内存生成一个新的数据版本，最后在持久化时合并所有版本分支。
 * 以上策略常被人总结为：Copy On Write，用以提高并发性
 *
 *
 * @author Yue Yu
 * @create 2018-03-26 下午3:06
 **/
public class MVBTreeMap<K,V> extends AbstractMap<K, V>
        implements ConcurrentMap<K, V> {

    protected BTreeForest bTree;

    /**
     * Reference to the current root page.
     */
    private final AtomicReference<RootReference> root;

    private int id;
    private long createVersion;
    private final DataType keyType;
    private final DataType valueType;

    private volatile  boolean closed;
    private boolean readOnly;
    private boolean isVolatile;

    /**
     * This designates the "last stored" version for a store which was
     * just open for the first time.
     */
    static final long INITIAL_VERSION = -1;

    protected MVBTreeMap(Map<String, Object> config) {
        this((BTreeForest) config.get("store"),
                (DataType) config.get("key"),
                (DataType) config.get("val"),
                DataUtils.readHexInt(config, "id", 0),
                DataUtils.readHexLong(config, "createVersion", 0),
                new AtomicReference<RootReference>()
        );
        setInitialRoot(createEmptyLeaf(), bTree.getCurrentVersion());
    }

    // constructor for cloneIt()
    protected MVBTreeMap(MVBTreeMap<K, V> source) {
        this(source.bTree, source.keyType, source.valueType, source.id, source.createVersion,
                new AtomicReference<>(source.root.get()));
    }

    // meta map constructor
    MVBTreeMap(BTreeForest bTree) {
        this(bTree, StringDataType.INSTANCE,StringDataType.INSTANCE, 0, 0, new AtomicReference<RootReference>());
        setInitialRoot(createEmptyLeaf(), bTree.getCurrentVersion());
    }

    private MVBTreeMap(BTreeForest bTree, DataType keyType, DataType valueType, int id, long createVersion,
                       AtomicReference<RootReference> root) {
        this.bTree = bTree;
        this.id = id;
        this.createVersion = createVersion;
        this.keyType = keyType;
        this.valueType = valueType;
        this.root = root;
    }

    static String getMapRootKey(int mapId) {
        return "root." + Integer.toHexString(mapId);
    }

    static String getMapKey(int mapId) {
        return "map." + Integer.toHexString(mapId);
    }

    protected void init() {}

    public Page createEmptyLeaf() {
        return Page.createEmptyLeaf(this);
    }

    final void setInitialRoot(Page rootPage, long version) {
        root.set(new RootReference(rootPage, version));
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, DecisionMaker.PUT);
    }

    public final V put(K key, V value, DecisionMaker<? super V> decisionMaker) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        return operate(key, value, decisionMaker);
    }


    public V operate(K key, V value, DecisionMaker<? super V> decisionMaker) {
        beforeWrite();
        int attempt = 0;
        RootReference oldRootReference = null;
        while(true) {
            RootReference rootReference = getRoot();
            int contention = 0;
            if (oldRootReference != null) {
                long updateAttemptCounter = rootReference.updateAttemptCounter -
                        oldRootReference.updateAttemptCounter;
                assert updateAttemptCounter >= 0 : updateAttemptCounter;
                long updateCounter = rootReference.updateCounter - oldRootReference.updateCounter;
                assert updateCounter >= 0 : updateCounter;
                assert updateAttemptCounter >= updateCounter : updateAttemptCounter + " >= " + updateCounter;
                contention = (int)((updateAttemptCounter+1) / (updateCounter+1));
            }
            oldRootReference = rootReference;
            ++attempt;
            CursorPos pos = traverseDown(rootReference.root, key);
            Page p = pos.page;
            int index = pos.index;
            CursorPos tip = pos;
            pos = pos.parent;
            @SuppressWarnings("unchecked")
            V result = index < 0 ? null : (V)p.getValue(index);
            Decision decision = decisionMaker.decide(result, value);

            int unsavedMemory = 0;
            boolean needUnlock = false;
            try {
                switch (decision) {
                    case ABORT:
                        if(rootReference != getRoot()) {
                            decisionMaker.reset();
                            continue;
                        }
                        return result;
                    case REMOVE: {
                        if (index < 0) {
                            return null;
                        }
                        if (attempt > 2 && !(needUnlock = lockRoot(decisionMaker, rootReference,
                                attempt, contention))) {
                            continue;
                        }
                        if (p.getTotalCount() == 1 && pos != null) {
                            p = pos.page;
                            index = pos.index;
                            pos = pos.parent;
                            if (p.getKeyCount() == 1) {
                                assert index <= 1;
                                p = p.getChildPage(1 - index);
                                break;
                            }
                            assert p.getKeyCount() > 1;
                        }
                        p = p.copy();
                        p.remove(index);
                        break;
                    }
                    case PUT: {
                        if (attempt > 2 && !(needUnlock = lockRoot(decisionMaker, rootReference,
                                attempt, contention))) {
                            continue;
                        }
                        value = decisionMaker.selectValue(result, value);
                        p = p.copy();
                        if (index < 0) {
                            p.insertLeaf(-index - 1, key, value);
                            int keyCount;
                            //如果节点keys的数量超过了bp树的(阶数-1),或者单个page的占用内存超标则自顶向下分裂
                            while ((keyCount = p.getKeyCount()) > bTree.getKeysPerPage()
                                    || p.getMemory() > bTree.getMaxPageSize()
                                    && keyCount > (p.isLeaf() ? 1 : 2)) {
                                long totalCount = p.getTotalCount();
                                int at = keyCount >> 1;
                                Object k = p.getKey(at);
                                Page split = p.split(at);
                                unsavedMemory += p.getMemory();
                                unsavedMemory += split.getMemory();
                                if (pos == null) {
                                    Object keys[] = { k };
                                    Page.PageReference children[] = {
                                            new Page.PageReference(p),
                                            new Page.PageReference(split)
                                    };
                                    p = Page.create(this, keys, null, children, totalCount, 0);
                                    break;
                                }
                                Page c = p;
                                p = pos.page;
                                index = pos.index;
                                pos = pos.parent;
                                p = p.copy();
                                p.setChild(index, split);
                                p.insertNode(index, k, c);
                            }
                        } else {
                            p.setValue(index, value);
                        }
                        break;
                    }
                }
                unsavedMemory += p.getMemory();
                while (pos != null) {
                    Page c = p;
                    p = pos.page;
                    p = p.copy();
                    p.setChild(pos.index, c);
                    unsavedMemory += p.getMemory();
                    pos = pos.parent;
                }
                if(needUnlock) {
                    unlockRoot(p, attempt);
                    needUnlock = false;
                } else if(!updateRoot(rootReference, p, attempt)) {
                    decisionMaker.reset();
                    continue;
                }
                while (tip != null) {
                    tip.page.removePage();
                    tip = tip.parent;
                }
                if (bTree.getFileStore() != null) {
                    bTree.registerUnsavedPage(unsavedMemory);
                }
                return result;
            } finally {
                if(needUnlock) {
                    unlockRoot(rootReference.root, attempt);
                }
            }
        }
    }

    private boolean lockRoot(DecisionMaker<? super V> decisionMaker, RootReference rootReference,
                             int attempt, int contention) {
        boolean success = lockRoot(rootReference);
        if (!success) {
            decisionMaker.reset();
            if(attempt > 4) {
                if (attempt <= 24) {
                    Thread.yield();
                } else {
                    try {
                        Thread.sleep(0, 100 / contention + 50);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        return success;
    }

    /**
     * 自顶向下搜索
     * @param p
     * @param key
     * @return
     */
    private static CursorPos traverseDown(Page p, Object key) {
        CursorPos pos = null;
        while (!p.isLeaf()) {
            assert p.getKeyCount() > 0;
            int index = p.binarySearch(key) + 1;
            if (index < 0) {
                index = -index;
            }
            pos = new CursorPos(p, index, pos);
            p = p.getChildPage(index);
        }
        return new CursorPos(p, p.binarySearch(key), pos);
    }

    private boolean lockRoot(RootReference rootReference) {
        return !rootReference.lockedForUpdate
                && root.compareAndSet(rootReference, new RootReference(rootReference));
    }

    private void unlockRoot(Page newRoot, int attempt) {
        boolean success;
        do {
            RootReference rootReference = getRoot();
            RootReference updatedRootReference = new RootReference(rootReference, newRoot, attempt);
            success = root.compareAndSet(rootReference, updatedRootReference);
        } while(!success);
    }

    /**
     * 更新root的引用信息至最新版本
     * @param oldRoot
     * @param newRoot
     * @param attemptUpdateCounter
     * @return
     */
    protected final boolean updateRoot(RootReference oldRoot, Page newRoot, int attemptUpdateCounter) {
        return setNewRoot(oldRoot, newRoot, attemptUpdateCounter, true) != null;
    }

    private RootReference setNewRoot(RootReference oldRoot, Page newRootPage,
                                     int attemptUpdateCounter, boolean obeyLock) {
        RootReference currentRoot = getRoot();
        assert newRootPage != null || currentRoot != null;
        if (currentRoot != oldRoot && oldRoot != null) {
            return null;
        }

        RootReference previous = currentRoot;
        long updateCounter = 1;
        long newVersion = INITIAL_VERSION;
        if(currentRoot != null) {
            if (obeyLock && currentRoot.lockedForUpdate) {
                return null;
            }

            if (newRootPage == null) {
                newRootPage = currentRoot.root;
            }

            newVersion = currentRoot.version;
            previous = currentRoot.previous;
            updateCounter += currentRoot.updateCounter;
            attemptUpdateCounter += currentRoot.updateAttemptCounter;
        }

        RootReference updatedRootReference = new RootReference(newRootPage, newVersion, previous, updateCounter,
                attemptUpdateCounter, false);
        boolean success = root.compareAndSet(currentRoot, updatedRootReference);
        return success ? updatedRootReference : null;
    }

    public final RootReference getRoot() {
        return root.get();
    }

    int compare(Object a, Object b) {
        return keyType.compare(a, b);
    }

    public int getId() {
        return id;
    }

    public BTreeForest getBTree() {
        return bTree;
    }


    void removeUnusedOldVersions(RootReference rootReference) {
        long oldest = bTree.getOldestVersionToKeep();
        // We need to keep at least one previous version (if any) here,
        // because in order to retain whole history of some version
        // we really need last root of the previous version.
        // Root labeled with version "X" is the LAST known root for that version
        // and therefore the FIRST known root for the version "X+1"
        for(RootReference rootRef = rootReference; rootRef != null; rootRef = rootRef.previous) {
            if (rootRef.version < oldest) {
                rootRef.previous = null;
            }
        }
    }


    /**
     * 检测map是否可写
     */
    protected void beforeWrite() {
        if (closed) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_CLOSED, "This map is closed");
        }
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException(
                    "This map is read-only");
        }
        bTree.beforeWrite(this);
    }

    protected void removePage(long pos, int memory) {
        bTree.removePage(this, pos, memory);
    }

    public final long getVersion() {
        RootReference rootReference = getRoot();
        RootReference previous = rootReference.previous;
        return previous == null || previous.root != rootReference.root ?
                rootReference.version : previous.version;
    }

    public boolean isClosed() {
        return closed;
    }

    public DataType getKeyType() {
        return keyType;
    }

    public DataType getValueType(){
        return valueType;
    }

    final RootReference setWriteVersion(long writeVersion) {
        int attempt = 0;
        while(true) {
            RootReference rootReference = getRoot();
            if(rootReference.version >= writeVersion) {
                return rootReference;
            } else if (isClosed()) {
                if (rootReference.version < bTree.getOldestVersionToKeep()) {
                    return null;
                }
                return rootReference;
            }
            RootReference updatedRootReference = new RootReference(rootReference, writeVersion, ++attempt);
            if(root.compareAndSet(rootReference, updatedRootReference)) {
                removeUnusedOldVersions(updatedRootReference);
                return updatedRootReference;
            }
        }
    }

    final void setRootPos(long rootPos, long version) {
        Page root = readOrCreateRootPage(rootPos);
        setInitialRoot(root, version);
        setWriteVersion(bTree.getCurrentVersion());
    }

    private Page readOrCreateRootPage(long rootPos) {
        Page root = rootPos == 0 ? createEmptyLeaf() : readPage(rootPos);
        return root;
    }

    final boolean hasChangesSince(long version) {
        return getVersion() > version;
    }


    Page readPage(long pos) {
        return bTree.readPage(this, pos);
    }


    public List<K> keyList() {
        return new AbstractList<K>() {

            @Override
            public K get(int index) {
                return getKey(index);
            }

            @Override
            public int size() {
                return MVBTreeMap.this.size();
            }

            @Override
            @SuppressWarnings("unchecked")
            public int indexOf(Object key) {
                return (int) getKeyIndex((K) key);
            }

        };
    }

    public final Iterator<K> keyIterator(K from) {
        return new Cursor<K, V>(getRootPage(), from);
    }

    public final Page getRootPage() {
        return getRoot().root;
    }

    /**
     * 回滚至某一版本
     * @param version
     */
    final void rollbackTo(long version) {
        // check if the map was removed and re-created later ?
        if (version > createVersion) {
            rollbackRoot(version);
        }
    }

    void rollbackRoot(long version)
    {
        RootReference rootReference = getRoot();
        RootReference previous;
        while (rootReference.version >= version && (previous = rootReference.previous) != null) {
            if (root.compareAndSet(rootReference, previous)) {
                rootReference = previous;
                closed = false;
            }
        }
        setWriteVersion(version);
    }


    public final K getKey(long index) {
        if (index < 0 || index >= sizeAsLong()) {
            return null;
        }
        Page p = getRootPage();
        long offset = 0;
        while (true) {
            if (p.isLeaf()) {
                if (index >= offset + p.getKeyCount()) {
                    return null;
                }
                @SuppressWarnings("unchecked")
                K key = (K) p.getKey((int) (index - offset));
                return key;
            }
            int i = 0, size = getChildPageCount(p);
            for (; i < size; i++) {
                long c = p.getCounts(i);
                if (index < c + offset) {
                    break;
                }
                offset += c;
            }
            if (i == size) {
                return null;
            }
            p = p.getChildPage(i);
        }
    }

    public final long sizeAsLong() {
        return getRootPage().getTotalCount();
    }

    public final long getKeyIndex(K key) {
        Page p = getRootPage();
        if (p.getTotalCount() == 0) {
            return -1;
        }
        long offset = 0;
        while (true) {
            int x = p.binarySearch(key);
            if (p.isLeaf()) {
                if (x < 0) {
                    offset = -offset;
                }
                return offset + x;
            }
            if (x++ < 0) {
                x = -x;
            }
            for (int i = 0; i < x; i++) {
                offset += p.getCounts(i);
            }
            p = p.getChildPage(x);
        }
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        V old = get(key);
        if (old == null) {
            put(key, value);
        }
        return old;
    }

    @Override
    public synchronized boolean remove(Object key,Object value){
        V old = get(key);
        if (areValuesEqual(old, value)) {
            remove(key);
            return true;
        }
        return false;
    }


    /**
     * 根据指定的序列化类型比较value的大小
     * @param a
     * @param b
     * @return
     */
    public boolean areValuesEqual(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return valueType.compare(a, b) == 0;
    }


    /**
     * 根据指定的key将指定的old value替换为new value
     * @param key
     * @param oldValue
     * @param newValue
     * @return
     */
    @Override
    public synchronized boolean replace(K key, V oldValue, V newValue) {
        V old = get(key);
        if (areValuesEqual(old, oldValue)) {
            put(key, newValue);
            return true;
        }
        return false;
    }

    public final boolean isVolatile() {
        return isVolatile;
    }

    /**
     * 根据指定的key替换value
     * @param key
     * @param value
     * @return
     */
    @Override
    public synchronized  V replace(K key,V value){
        V old = get(key);
        if (old != null) {
            put(key, value);
            return old;
        }
        return null;
    }

    @Override
    public final Set<Map.Entry<K, V>> entrySet() {
        final Page root = this.getRootPage();
        return new AbstractSet<Entry<K, V>>() {

            @Override
            public Iterator<Entry<K, V>> iterator() {
                final Cursor<K, V> cursor = new Cursor<>(root, null);
                return new Iterator<Entry<K, V>>() {

                    @Override
                    public boolean hasNext() {
                        return cursor.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        K k = cursor.next();
                        return new SimpleImmutableEntry<>(k, cursor.getValue());
                    }

                    @Override
                    public void remove() {
                        throw DataUtils.newUnsupportedOperationException(
                                "Removing is not supported");
                    }
                };

            }

            @Override
            public int size() {
                return MVBTreeMap.this.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVBTreeMap.this.containsKey(o);
            }

        };

    }

    final void rewrite(Set<Integer> set) {
        rewrite(getRootPage(), set);
    }

    private int rewrite(Page p, Set<Integer> set) {
        if (p.isLeaf()) {
            long pos = p.getPos();
            int chunkId = DataUtils.getPageChunkId(pos);
            if (!set.contains(chunkId)) {
                return 0;
            }
            if (p.getKeyCount() > 0) {
                @SuppressWarnings("unchecked")
                K key = (K) p.getKey(0);
                V value = get(key);
                if (value != null) {
                    replace(key, value, value);
                }
            }
            return 1;
        }
        int writtenPageCount = 0;
        for (int i = 0; i < getChildPageCount(p); i++) {
            long childPos = p.getChildPagePos(i);
            if (childPos != 0 && DataUtils.getPageType(childPos) == DataUtils.PAGE_TYPE_LEAF) {
                //dfs到处于set指定chunkId的leaf page，rewrite
                int chunkId = DataUtils.getPageChunkId(childPos);
                if (!set.contains(chunkId)) {
                    continue;
                }
            }
            writtenPageCount += rewrite(p.getChildPage(i), set);
        }
        if (writtenPageCount == 0) {
            long pos = p.getPos();
            int chunkId = DataUtils.getPageChunkId(pos);
            if (set.contains(chunkId)) {
                // 如果某个内部节点的指针指向set中的某个chunk，那么找到它的一个叶子节点重写来更新整个chunk
                Page p2 = p;
                while (!p2.isLeaf()) {
                    p2 = p2.getChildPage(0);
                }
                @SuppressWarnings("unchecked")
                K key = (K) p2.getKey(0);
                V value = get(key);
                if (value != null) {
                    replace(key, value, value);
                }
                writtenPageCount++;
            }
        }
        return writtenPageCount;
    }


    public final MVBTreeMap<K, V> openVersion(long version) {
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException(
                    "This map is read-only; need to call " +
                            "the method on the writable map");
        }
        DataUtils.checkArgument(version >= createVersion,
                "Unknown version {0}; this map was created in version is {1}",
                version, createVersion);
        RootReference rootReference = getRoot();
        removeUnusedOldVersions(rootReference);
        while (rootReference != null && rootReference.version > version) {
            rootReference = rootReference.previous;
        }

        if (rootReference == null) {
            // smaller than all in-memory versions
            MVBTreeMap<K, V> map = openReadOnly(bTree.getRootPos(getId(), version), version);
            return map;
        }
        MVBTreeMap<K, V> m = openReadOnly(rootReference.root, version);
        assert m.getVersion() <= version : m.getVersion() + " <= " + version;
        return m;
    }

    final MVBTreeMap<K, V> openReadOnly(long rootPos, long version) {
        Page root = readOrCreateRootPage(rootPos);
        return openReadOnly(root, version);
    }

    private MVBTreeMap<K, V> openReadOnly(Page root, long version) {
        MVBTreeMap<K, V> m = cloneIt();
        m.readOnly = true;
        m.setInitialRoot(root, version);
        return m;
    }

    protected MVBTreeMap<K, V> cloneIt() {
        return new MVBTreeMap<>(this);
    }

    public final Cursor<K, V> cursor(K from) {
        return new Cursor<>(getRootPage(), from);
    }

    void close() {
        closed = true;
    }

    public long getCreateVersion() {
        return createVersion;
    }

    protected int getChildPageCount(Page p) {
        return p.getRawChildPageCount();
    }

    public static final class RootReference
    {
        /**
         * 根节点
         */
        public  final    Page          root;
        /**
         * 写时版本
         */
        public  final    long          version;
        /**
         * 是否已在update时被锁
         */
        final    boolean       lockedForUpdate;
        /**
         * 链表中前一个根节点的引用
         */
        public  volatile RootReference previous;
        /**
         * update成功次数
         */
        public  final    long          updateCounter;
        /**
         * update尝试次数
         */
        public  final    long          updateAttemptCounter;

        RootReference(Page root, long version, RootReference previous,
                      long updateCounter, long updateAttemptCounter,
                      boolean lockedForUpdate) {
            this.root = root;
            this.version = version;
            this.previous = previous;
            this.updateCounter = updateCounter;
            this.updateAttemptCounter = updateAttemptCounter;
            this.lockedForUpdate = lockedForUpdate;
        }

        // This one is used for locking
        RootReference(RootReference r) {
            this.root = r.root;
            this.version = r.version;
            this.previous = r.previous;
            this.updateCounter = r.updateCounter;
            this.updateAttemptCounter = r.updateAttemptCounter;
            this.lockedForUpdate = true;
        }

        // This one is used for unlocking
        RootReference(RootReference r, Page root, int attempt) {
            this.root = root;
            this.version = r.version;
            this.previous = r.previous;
            this.updateCounter = r.updateCounter + 1;
            this.updateAttemptCounter = r.updateAttemptCounter + attempt;
            this.lockedForUpdate = false;
        }

        // This one is used for version change
        RootReference(RootReference r, long version, int attempt) {
            RootReference previous = r;
            RootReference tmp;
            while ((tmp = previous.previous) != null && tmp.root == r.root) {
                previous = tmp;
            }
            this.root = r.root;
            this.version = version;
            this.previous = previous;
            this.updateCounter = r.updateCounter + 1;
            this.updateAttemptCounter = r.updateAttemptCounter + attempt;
            this.lockedForUpdate = r.lockedForUpdate;
        }

        // This one is used for r/o snapshots
        RootReference(Page root, long version) {
            this.root = root;
            this.version = version;
            this.previous = null;
            this.updateCounter = 1;
            this.updateAttemptCounter = 1;
            this.lockedForUpdate = false;
        }

        @Override
        public String toString() {
            return "RootReference("+ System.identityHashCode(root)+","+version+","+ lockedForUpdate +")";
        }
    }

    public enum Decision { ABORT, REMOVE, PUT }

    /**
     * Class DecisionMaker为MVMap.operate方法提供回调接口来决定决定具体的write策略
     *
     * @param <V> value type of the map
     */
    public abstract static class DecisionMaker<V>
    {

        public abstract Decision decide(V existingValue, V providedValue);

        public static final DecisionMaker<Object> DEFAULT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return providedValue == null ? Decision.REMOVE : Decision.PUT;
            }

            @Override
            public String toString() {
                return "default";
            }
        };

        public static final DecisionMaker<Object> PUT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return Decision.PUT;
            }

            @Override
            public String toString() {
                return "put";
            }
        };

        public static final DecisionMaker<Object> REMOVE = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return Decision.REMOVE;
            }

            @Override
            public String toString() {
                return "remove";
            }
        };

        static final DecisionMaker<Object> IF_ABSENT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return existingValue == null ? Decision.PUT : Decision.ABORT;
            }

            @Override
            public String toString() {
                return "if_absent";
            }
        };

        static final DecisionMaker<Object> IF_PRESENT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return existingValue != null ? Decision.PUT : Decision.ABORT;
            }

            @Override
            public String toString() {
                return "if_present";
            }
        };


        public <T extends V> T selectValue(T existingValue, T providedValue) {
            return providedValue;
        }

        public void reset() {}
    }

    public interface MapBuilder<M extends MVBTreeMap<K, V>, K, V> {

        M create(BTreeForest bTree, Map<String, Object> config);

        DataType getKeyType();

        DataType getValueType();

        void setKeyType(DataType dataType);

        void setValueType(DataType dataType);

    }

    public abstract static class BasicBuilder<M extends MVBTreeMap<K, V>, K, V> implements MapBuilder<M, K, V> {

        private DataType keyType;
        private DataType valueType;

        protected BasicBuilder() {
        }

        @Override
        public DataType getKeyType() {
            return keyType;
        }

        @Override
        public DataType getValueType() {
            return valueType;
        }

        @Override
        public void setKeyType(DataType keyType) {
            this.keyType = keyType;
        }

        @Override
        public void setValueType(DataType valueType) {
            this.valueType = valueType;
        }

        public BasicBuilder<M, K, V> keyType(DataType keyType) {
            this.keyType = keyType;
            return this;
        }

        public BasicBuilder<M, K, V> valueType(DataType valueType) {
            this.valueType = valueType;
            return this;
        }

        @Override
        public M create(BTreeForest btree, Map<String, Object> config) {
            if (getKeyType() == null) {
                setKeyType(new ObjectDataType());
            }
            if (getValueType() == null) {
                setValueType(new ObjectDataType());
            }
            DataType keyType = getKeyType();
            DataType valueType = getValueType();
            config.put("store", btree);
            config.put("key", keyType);
            config.put("val", valueType);
            return create(config);
        }

        protected abstract M create(Map<String, Object> config);

    }

    public static class Builder<K, V> extends BasicBuilder<MVBTreeMap<K, V>, K, V> {

        public Builder() {}

        @Override
        public Builder<K,V> keyType(DataType dataType) {
            setKeyType(dataType);
            return this;
        }

        @Override
        public Builder<K, V> valueType(DataType dataType) {
            setValueType(dataType);
            return this;
        }

        @Override
        protected MVBTreeMap<K, V> create(Map<String, Object> config) {
            Object type = config.get("type");
            if(type == null || type.equals("rtree")) {
                return new MVBTreeMap<>(config);
            }
            throw new IllegalArgumentException("Incompatible map type");
        }
    }

    protected String asString(String name) {
        StringBuilder buff = new StringBuilder();
        if (name != null) {
            DataUtils.appendMap(buff, "name", name);
        }
        if (createVersion != 0) {
            DataUtils.appendMap(buff, "createVersion", createVersion);
        }
        String type = getType();
        if (type != null) {
            DataUtils.appendMap(buff, "type", type);
        }
        return buff.toString();
    }

    public String getType() {
        return null;
    }

}
