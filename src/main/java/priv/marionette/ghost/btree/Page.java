package priv.marionette.ghost.btree;

import priv.marionette.compress.Compressor;
import priv.marionette.ghost.WriteBuffer;
import priv.marionette.ghost.type.DataType;
import priv.marionette.tools.DataUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static priv.marionette.engine.Constants.MEMORY_ARRAY;
import static priv.marionette.engine.Constants.MEMORY_OBJECT;
import static priv.marionette.engine.Constants.MEMORY_POINTER;
import static priv.marionette.tools.DataUtils.PAGE_TYPE_LEAF;

/**
 * B树的节点
 * 文件数据结构按照算法导论(第三版)给出如下定义:
 * 每个B树的节点拥有一个左子节点，这个节点的key大于子节点key list的最大值
 *
 * 结构化文件(offset从小到大)：
 * page length: int
 * checksum : short
 * map id : varInt
 * number of keys :varInt
 * type : (0: 叶子节点, 1: 内部或根节点; +2: compressed)
 * compressed: bytes saved (varInt) keys{
 * leaf: values (one for each key)
 * node: children (1 more than keys)
 * }
 *
 * @author Yue Yu
 * @create 2018-03-13 下午4:35
 **/
public abstract class Page implements Cloneable{


    public  final MVMap<?, ?> map;

    private long pos;

    private int cachedCompare;

    private int memory;

    private Object[] keys;

    private Object[] values;

    private PageReference[] children;

    private volatile boolean removedInMemory;

    /**
     *    每一个子节点的引用至少占用的内存
     */
    static final int PAGE_MEMORY_CHILD = MEMORY_POINTER + 16;

    /**
     *  每一个基础page所至少要占用的内存
     */
    private static final int PAGE_MEMORY =
            MEMORY_OBJECT +           // this
            2 * MEMORY_POINTER +      // map, keys
            MEMORY_ARRAY +            // Object[] keys
            17;                       // pos, cachedCompare, memory, removedInMemory

    /**
     * 每一个内部节点所至少要占用的内存
     */
    static final int PAGE_NODE_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // children
            MEMORY_ARRAY +            // Object[] children
            8;                        // totalCount


    /**
     *  每一个叶子节点所至少要占用的内存
     */
    static final int PAGE_LEAF_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // values
            MEMORY_ARRAY;             // Object[] values


    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private static final int IN_MEMORY = 0x80000000;

    private static final PageReference[] SINGLE_EMPTY = { PageReference.EMPTY };


    Page(MVMap<?, ?> map) {
        this.map = map;
    }

    Page(MVMap<?, ?> map, Page source) {
        this(map, source.keys);
        memory = source.memory;
    }

    Page(MVMap<?, ?> map, Object keys[]) {
        this.map = map;
        this.keys = keys;
    }

    /**
     * 创建一个新的空叶子节点
     *
     * @param map the map
     * @return the new page
     */
    static Page createEmptyLeaf(MVMap<?, ?> map) {
        Page page = new Leaf(map, EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY);
        page.initMemoryAccount(PAGE_LEAF_MEMORY);
        return page;
    }

    /**
     * 创建一个新的空非叶子节点
     * @param map
     * @return
     */
    public static Page createEmptyNode(MVMap<?, ?> map) {
        Page page = new NonLeaf(map, EMPTY_OBJECT_ARRAY, SINGLE_EMPTY, 0);
        page.initMemoryAccount(PAGE_NODE_MEMORY +
                MEMORY_POINTER + PAGE_MEMORY_CHILD); // there is always one child
        return page;
    }


    /**
     * create a new page
     * @param map
     * @param keys
     * @param values
     * @param children
     * @param totalCount
     * @param memory
     * @return
     */
    public static Page create(MVMap<?, ?> map,
                              Object[] keys, Object[] values, PageReference[] children,
                              long totalCount, int memory) {
        assert keys != null;
        Page p = children == null ? new Leaf(map, keys, values) :
                new NonLeaf(map, keys, children, totalCount);
        p.initMemoryAccount(memory);
        return p;
    }

    private void initMemoryAccount(int memoryCount) {
        if(map.bTree.getFileStore() == null) {
            memory = IN_MEMORY;
        } else if (memoryCount == 0) {
            recalculateMemory();
        } else {
            addMemory(memoryCount);
            assert memoryCount == getMemory();
        }
    }

    protected void addMemory(int mem) {
        memory += mem;
    }


    public static Object get(Page p, Object key) {
        while (true) {
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                return index >= 0 ? p.getValue(index) : null;
            } else if (index++ < 0) {
                index = -index;
            }
            p = p.getChildPage(index);
        }
    }

    /**
     * 计算节点key array所占用的内存
     * @return
     */
    protected int calculateMemory() {
        int mem = keys.length * MEMORY_POINTER;
        DataType keyType = map.getKeyType();
        for (Object key : keys) {
            mem += keyType.getMemory(key);
        }
        return mem;
    }

    protected void recalculateMemory() {
        int mem = DataUtils.PAGE_MEMORY;
        DataType keyType = map.getKeyType();
        for (int i = 0; i < keys.length; i++) {
            mem += keyType.getMemory(keys[i]);
        }
        if (this.isLeaf()) {
            DataType valueType = map.getValueType();
            for (int i = 0; i < keys.length; i++) {
                mem += valueType.getMemory(values[i]);
            }
        } else {
            mem += this.getRawChildPageCount() * DataUtils.PAGE_MEMORY_CHILD;
        }
        addMemory(mem - memory);
    }


    public int getMemory() {
        if (isPersistent()) {
            if (BTreeWithMVCC.ASSERT) {
                int mem = memory;
                recalculateMemory();
                if (mem != memory) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_INTERNAL, "Memory calculation error");
                }
            }
            return memory;
        }
        return getKeyCount();
    }

    public int getKeyCount() {
        return keys.length;
    }

    public void remove(int index) {
        int keyCount = getKeyCount();
        DataType keyType = map.getKeyType();
        if (index == keyCount) {
            --index;
        }
        if(isPersistent()) {
            Object old = getKey(index);
            addMemory(-MEMORY_POINTER - keyType.getMemory(old));
        }
        Object newKeys[] = new Object[keyCount - 1];
        DataUtils.copyExcept(keys, newKeys, keyCount, index);
        keys = newKeys;
    }

    public int getRawChildPageCount() {
        return children.length;
    }


    public void removePage() {
        if(isPersistent()) {
            long p = pos;
            if (p == 0) {
                removedInMemory = true;
            }
            map.removePage(p, memory);
        }
    }

    protected boolean isPersistent() {
        return memory != IN_MEMORY;
    }


    public final long getPos() {
        return pos;
    }


    /**
     * 从磁盘上读取b树的一个节点
     * @param fileStore
     * @param pos
     * @param map
     * @param filePos
     * @param maxPos
     * @return
     */
    static Page read(FileStore fileStore, long pos, MVMap<?, ?> map,
                     long filePos, long maxPos) {
        ByteBuffer buff;
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            buff = fileStore.readFully(filePos, 128);
            maxLength = buff.getInt();
            // read the first bytes again
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ",
                    length, filePos, maxPos);
        }
        buff = fileStore.readFully(filePos, length);
        boolean leaf = (DataUtils.getPageType(pos) & 1) == PAGE_TYPE_LEAF;
        Page p = leaf ? new Leaf(map) : new NonLeaf(map);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }


    private void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}",
                    chunkId, maxLength, pageLength);
        }
        buff.limit(start + pageLength);
        short check = buff.getShort();
        int mapId = DataUtils.readVarInt(buff);
        if (mapId != map.getId()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}",
                    chunkId, map.getId(), mapId);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}",
                    chunkId, checkTest, check);
        }
        int len = DataUtils.readVarInt(buff);
        keys = new Object[len];
        int type = buff.get();
        if(isLeaf() != ((type & 1) == PAGE_TYPE_LEAF)) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected node type {1}, got {2}",
                    chunkId, isLeaf() ? "0" : "1" , type);
        }
        if (!isLeaf()) {
            readPayLoad(buff);
        }
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & DataUtils.PAGE_COMPRESSED_HIGH) ==
                    DataUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getBTree().getCompressorHigh();
            } else {
                compressor = map.getBTree().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = DataUtils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, buff.array(),
                    buff.arrayOffset(), l);
        }
        map.getKeyType().read(buff, keys, len, true);
        if (isLeaf()) {
            readPayLoad(buff);
        }
        recalculateMemory();
    }


    /**
     * 读取一个内部节点的文件指针信息
     * @param fileStore
     * @param pos
     * @param filePos
     * @param maxPos
     * @param collector
     */
    static void readChildrenPositions(FileStore fileStore, long pos,
                                      long filePos, long maxPos,
                                      BTreeWithMVCC.ChunkIdsCollector collector) {
        ByteBuffer buff;
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            buff = fileStore.readFully(filePos, 128);
            maxLength = buff.getInt();
            // read the first bytes again
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ",
                    length, filePos, maxPos);
        }
        buff = fileStore.readFully(filePos, length);
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length =< {1}, got {2}",
                    chunkId, maxLength, pageLength);
        }
        buff.limit(start + pageLength);
        short check = buff.getShort();
        int m = DataUtils.readVarInt(buff);
        int mapId = collector.getMapId();
        if (m != mapId) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}",
                    chunkId, mapId, m);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}",
                    chunkId, checkTest, check);
        }
        int len = DataUtils.readVarInt(buff);
        int type = buff.get();
        if ((type & 1) != DataUtils.PAGE_TYPE_NODE) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Position {0} expected to be a non-leaf", pos);
        }
        for (int i = 0; i <= len; i++) {
            collector.visit(buff.getLong());
        }
    }

    protected abstract void readPayLoad(ByteBuffer buff);


    /**
     * 向指定chunk持久化b树节点
     * @param chunk
     * @param buff
     * @return
     */
    protected final int write(Chunk chunk, WriteBuffer buff) {
        int start = buff.position();
        int len = getKeyCount();
        int type = isLeaf() ? PAGE_TYPE_LEAF : DataUtils.PAGE_TYPE_NODE;
        buff.putInt(0).
                putShort((byte) 0).
                putVarInt(map.getId()).
                putVarInt(len);
        int typePos = buff.position();
        buff.put((byte) type);
        writeChildren(buff, true);
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, getKeyCount(), true);
        writeValues(buff);
        BTreeWithMVCC bTree = map.getBTree();
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            int compressionLevel = bTree.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = map.getBTree().getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = map.getBTree().getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] exp = new byte[expLen];
                buff.position(compressStart).get(exp);
                byte[] comp = new byte[expLen * 2];
                int compLen = compressor.compress(exp, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(compLen - expLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos).
                            put((byte) (type + compressType));
                    buff.position(compressStart).
                            putVarInt(expLen - compLen).
                            put(comp, 0, compLen);
                }
            }
        }
        int pageLength = buff.position() - start;
        int chunkId = chunk.id;
        int check = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putInt(start, pageLength).
                putShort(start + 4, (short) check);
        if (isSaved()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = DataUtils.getPagePos(chunkId, start, pageLength, type);
        bTree.cachePage(this);
        if (type == DataUtils.PAGE_TYPE_NODE) {
            // cache again - this will make sure nodes stays in the cache
            // for a longer time
            bTree.cachePage(this);
        }
        long max = DataUtils.getPageMaxLength(pos);
        chunk.maxLen += max;
        chunk.maxLenLive += max;
        chunk.pageCount++;
        chunk.pageCountLive++;
        if (removedInMemory) {
            // if the page was removed _before_ the position was assigned, we
            // need to mark it removed here, so the fields are updated
            // when the next chunk is stored
            map.removePage(pos, memory);
        }
        return typePos + 1;
    }

    abstract void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff);

    public abstract Page getChildPageIfLoaded(int index);

    /**
     * 返回page的map owner id
     * @return id
     */
    public final int getMapId() {
        return map.getId();
    }

    /**
     * 获取子树的节点总数
     * @return
     */
    public abstract long getTotalCount();


    /**
     * 写时复制，clone一个完全相同的新page对象和owning map对象，clear其中对子节点的引用，子树置空，然后根据dfs做子树全拷贝
     * @param map
     * @return
     */
    abstract Page copy(MVMap<?, ?> map);


    public Object getKey(int index) {
        return keys[index];
    }


    abstract Page split(int at);

    /**
     * 分裂节点key array
     * @param aCount
     * @param bCount
     * @return
     */
    final Object[] splitKeys(int aCount, int bCount) {
        assert aCount + bCount <= getKeyCount();
        Object aKeys[] = createKeyStorage(aCount);
        Object bKeys[] = createKeyStorage(bCount);
        System.arraycopy(keys, 0, aKeys, 0, aCount);
        System.arraycopy(keys, getKeyCount() - bCount, bKeys, 0, bCount);
        keys = aKeys;
        return bKeys;
    }

    private Object[] createKeyStorage(int size)
    {
        return new Object[size];
    }

    final Object[] createValueStorage(int size)
    {
        return new Object[size];
    }


    /**
     * 记忆化二分搜索，将最近的一次搜索结果的index保存起来，
     * 以此应对相同字段的冗余搜索,这样每次搜索的起点并不同
     * 于常规的二分搜索(中点)。所以，所谓的算法是一种思想，
     * 是一种从计算机的角度看世界的方法，而不是现在很多人所
     * 理解的刻板的"模型"。
     * @param key
     * @return
     */
    public int binarySearch(Object key){
        int low =0,high = keys.length -1;
        int x = cachedCompare-1;
        if(x < 0 || x > high){
            x = high >>> 1;
        }
        while (low <= high){
            int result = map.compare(key,keys[x]);
            if(result > 0){
                low = x+1;
            }
            else  if(result < 0){
                high = x-1;
            }else {
                cachedCompare = x +1;
                return x;
            }
            x = (low+high) >>> 1;
        }
        cachedCompare = low;
        return -(low+1);
    }

    /**
     * 往叶子节点添加键值对
     *
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public abstract void insertLeaf(int index, Object key, Object value);

    public Object setValue(int index, Object value) {
        Object old = values[index];
        //深拷贝
        values = values.clone();
        DataType valueType = map.getValueType();
        if(isPersistent()) {
            addMemory(valueType.getMemory(value) -
                    valueType.getMemory(old));
        }
        values[index] = value;
        return old;
    }

    public Object getValue(int index) {
        return values[index];
    }


    public abstract void setChild(int index, Page c);

    /**
     * 向指定节点插入子节点
     * @param index
     * @param key
     * @param childPage
     */
    public abstract void insertNode(int index, Object key, Page childPage);

    /**
     * 获取指定index的左子节点
     * @param index
     * @return
     */
    public abstract Page getChildPage(int index);

    final void insertKey(int index, Object key) {
        int keyCount = getKeyCount();
        assert index <= keyCount : index + " > " + keyCount;
        Object[] newKeys = new Object[keyCount + 1];
        DataUtils.copyWithGap(keys, newKeys, keyCount, index);
        keys = newKeys;

        keys[index] = key;

        if (isPersistent()) {
            addMemory(MEMORY_POINTER + map.getKeyType().getMemory(key));
        }
    }

    long getCounts(int index) {
        return children[index].count;
    }

    public long getChildPagePos(int index) {
        return children[index].pos;
    }

    public final boolean isLeaf() {
        return getNodeType() == PAGE_TYPE_LEAF;
    }

    public abstract int getNodeType();

    public final boolean isSaved() {
        return DataUtils.isPageSaved(pos);
    }

    abstract void writeEnd();

    protected abstract void writeValues(WriteBuffer buff);

    protected abstract void writeChildren(WriteBuffer buff, boolean withCounts);

    public abstract void removeAllRecursive();


    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        dump(buff);
        return buff.toString();
    }

    protected void dump(StringBuilder buff) {
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("pos: ").append(Long.toHexString(pos)).append('\n');
        if (isSaved()) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append('\n');
        }
    }


    private static class Leaf extends Page
    {
        /**
         * 叶子节点的数据
         */
        private Object values[];

        Leaf(MVMap<?, ?> map) {
            super(map);
        }

        private Leaf(MVMap<?, ?> map, Leaf source) {
            super(map, source);
            this.values = source.values;
        }

        Leaf(MVMap<?, ?> map, Object keys[], Object values[]) {
            super(map, keys);
            this.values = values;
        }

        @Override
        public int getNodeType() {
            return PAGE_TYPE_LEAF;
        }

        @Override
        public Page copy(MVMap<?, ?> map) {
            return new Leaf(map, this);
        }

        @Override
        public Page getChildPage(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page getChildPageIfLoaded(int index) { throw new UnsupportedOperationException(); }

        @Override
        public long getChildPagePos(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        @SuppressWarnings("SuspiciousSystemArraycopy")
        public Page split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            Object bKeys[] = splitKeys(at, b);
            Object bValues[] = createValueStorage(b);
            if(values != null) {
                Object aValues[] = createValueStorage(at);
                System.arraycopy(values, 0, aValues, 0, at);
                System.arraycopy(values, at, bValues, 0, b);
                values = aValues;
            }
            Page newPage = create(super.map, bKeys, bValues, null, b, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public long getTotalCount() {
            return getKeyCount();
        }

        @Override
        long getCounts(int index) {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setChild(int index, Page c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object setValue(int index, Object value) {
            DataType valueType = map.getValueType();
            values = values.clone();
            Object old = setValueInternal(index, value);
            if(isPersistent()) {
                addMemory(valueType.getMemory(value) -
                        valueType.getMemory(old));
            }
            return old;
        }

        private Object setValueInternal(int index, Object value) {
            Object old = values[index];
            values[index] = value;
            return old;
        }

        @Override
        public void insertLeaf(int index, Object key, Object value) {
            int keyCount = getKeyCount();
            insertKey(index, key);

            if(values != null) {
                Object newValues[] = createValueStorage(keyCount + 1);
                DataUtils.copyWithGap(values, newValues, keyCount, index);
                values = newValues;
                setValueInternal(index, value);
                if (isPersistent()) {
                    addMemory(MEMORY_POINTER + map.getValueType().getMemory(value));
                }
            }
        }
        @Override
        public void insertNode(int index, Object key, Page childPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove(int index) {
            int keyCount = getKeyCount();
            super.remove(index);
            if (values != null) {
                if(isPersistent()) {
                    Object old = getValue(index);
                    addMemory(-MEMORY_POINTER - map.getValueType().getMemory(old));
                }
                Object newValues[] = createValueStorage(keyCount - 1);
                DataUtils.copyExcept(values, newValues, keyCount, index);
                values = newValues;
            }
        }

        @Override
        public void removeAllRecursive() {
            removePage();
        }

        @Override
        protected void readPayLoad(ByteBuffer buff) {
            int keyCount = getKeyCount();
            values = createValueStorage(keyCount);
            map.getValueType().read(buff, values, getKeyCount(), false);
        }

        @Override
        protected void writeValues(WriteBuffer buff) {
            map.getValueType().write(buff, values, getKeyCount(), false);
        }

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {}

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
            if (!isSaved()) {
                write(chunk, buff);
            }
        }

        @Override
        void writeEnd() {}

        @Override
        public int getRawChildPageCount() {
            return 0;
        }

        @Override
        protected int calculateMemory() {
            int mem = super.calculateMemory() + PAGE_LEAF_MEMORY +
                    values.length * MEMORY_POINTER;
            DataType valueType = map.getValueType();
            for (Object value : values) {
                mem += valueType.getMemory(value);
            }
            return mem;
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append(getKey(i));
                if (values != null) {
                    buff.append(':');
                    buff.append(getValue(i));
                }
            }
        }
    }

    private static final class NonLeaf extends Page
    {
        /**
         * The child page references.
         */
        private PageReference[] children;

        /**
         * The total entry count of this page and all children.
         */
        private long totalCount;

        NonLeaf(MVMap<?, ?> map) {
            super(map);
        }

        private NonLeaf(MVMap<?, ?> map, NonLeaf source, PageReference children[], long totalCount) {
            super(map, source);
            this.children = children;
            this.totalCount = totalCount;
        }

        NonLeaf(MVMap<?, ?> map, Object keys[], PageReference children[], long totalCount) {
            super(map, keys);
            this.children = children;
            this.totalCount = totalCount;
        }

        @Override
        public int getNodeType() {
            return DataUtils.PAGE_TYPE_NODE;
        }

        @Override
        public Page copy(MVMap<?, ?> map) {
            // replace child pages with empty pages
            PageReference[] children = new PageReference[this.children.length];
            Arrays.fill(children, PageReference.EMPTY);
            return new NonLeaf(map, this, children, 0);
        }

        @Override
        public Page getChildPage(int index) {
            PageReference ref = children[index];
            Page page = ref.page;
            if(page == null) {
                page = map.readPage(ref.pos);
                assert ref.pos == page.getPos();
                assert ref.count == page.getTotalCount();
            }
            return page;
        }

        @Override
        public Page getChildPageIfLoaded(int index) {
            return children[index].page;
        }

        @Override
        public long getChildPagePos(int index) {
            return children[index].pos;
        }

        @Override
        public Object getValue(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("SuspiciousSystemArraycopy")
        public Page split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            Object bKeys[] = splitKeys(at, b - 1);
            PageReference[] aChildren = new PageReference[at + 1];
            PageReference[] bChildren = new PageReference[b];
            System.arraycopy(children, 0, aChildren, 0, at + 1);
            System.arraycopy(children, at + 1, bChildren, 0, b);
            children = aChildren;

            long t = 0;
            for (PageReference x : aChildren) {
                t += x.count;
            }
            totalCount = t;
            t = 0;
            for (PageReference x : bChildren) {
                t += x.count;
            }
            Page newPage = create(map, bKeys, null, bChildren, t, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public long getTotalCount() {
            assert totalCount == calculateTotalCount() :
                    "Total count: " + totalCount + " != " + calculateTotalCount();
            return totalCount;
        }

        private long calculateTotalCount() {
            long check = 0;
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                check += children[i].count;
            }
            return check;
        }

        @Override
        long getCounts(int index) {
            return children[index].count;
        }

        @Override
        public void setChild(int index, Page c) {
            assert c != null;
            PageReference child = children[index];
            if (c != child.page || c.getPos() != child.pos) {
                totalCount += c.getTotalCount() - child.count;
                children = children.clone();
                children[index] = new PageReference(c);
            }
        }

        @Override
        public Object setValue(int index, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertLeaf(int index, Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertNode(int index, Object key, Page childPage) {
            int childCount = getRawChildPageCount();
            insertKey(index, key);

            PageReference newChildren[] = new PageReference[childCount + 1];
            DataUtils.copyWithGap(children, newChildren, childCount, index);
            children = newChildren;
            children[index] = new PageReference(childPage);

            totalCount += childPage.getTotalCount();
            if (isPersistent()) {
                addMemory(MEMORY_POINTER + PAGE_MEMORY_CHILD);
            }
        }

        @Override
        public void remove(int index) {
            int childCount = getRawChildPageCount();
            super.remove(index);
            if(isPersistent()) {
                addMemory(-MEMORY_POINTER - PAGE_MEMORY_CHILD);
            }
            totalCount -= children[index].count;
            PageReference newChildren[] = new PageReference[childCount - 1];
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;
        }

        @Override
        public void removeAllRecursive() {
            if (isPersistent()) {
                for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                    PageReference ref = children[i];
                    if (ref.page != null) {
                        ref.page.removeAllRecursive();
                    } else {
                        long c = children[i].pos;
                        int type = DataUtils.getPageType(c);
                        if (type == PAGE_TYPE_LEAF) {
                            int mem = DataUtils.getPageMaxLength(c);
                            map.removePage(c, mem);
                        } else {
                            map.readPage(c).removeAllRecursive();
                        }
                    }
                }
            }
            removePage();
        }

        @Override
        protected void readPayLoad(ByteBuffer buff) {
            int keyCount = getKeyCount();
            children = new PageReference[keyCount + 1];
            long p[] = new long[keyCount + 1];
            for (int i = 0; i <= keyCount; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= keyCount; i++) {
                long s = DataUtils.readVarLong(buff);
                long position = p[i];
                assert position == 0 ? s == 0 : s >= 0;
                total += s;
                children[i] = position == 0 ? PageReference.EMPTY : new PageReference(position, s);
            }
            totalCount = total;
        }

        @Override
        protected void writeValues(WriteBuffer buff) {}

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                buff.putLong(children[i].pos);
            }
            if(withCounts) {
                for (int i = 0; i <= keyCount; i++) {
                    buff.putVarLong(children[i].count);
                }
            }
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
            if (!isSaved()) {
                int patch = write(chunk, buff);
                int len = getRawChildPageCount();
                for (int i = 0; i < len; i++) {
                    Page p = children[i].page;
                    if (p != null) {
                        p.writeUnsavedRecursive(chunk, buff);
                        children[i] = new PageReference(p);
                    }
                }
                int old = buff.position();
                buff.position(patch);
                writeChildren(buff, false);
                buff.position(old);
            }
        }

        @Override
        void writeEnd() {
            int len = getRawChildPageCount();
            for (int i = 0; i < len; i++) {
                PageReference ref = children[i];
                if (ref.page != null) {
                    if (!ref.page.isSaved()) {
                        throw DataUtils.newIllegalStateException(
                                DataUtils.ERROR_INTERNAL, "Page not written");
                    }
                    ref.page.writeEnd();
                    children[i] = new PageReference(ref.pos, ref.count);
                }
            }
        }

        @Override
        public int getRawChildPageCount() {
            return getKeyCount() + 1;
        }

        @Override
        protected int calculateMemory() {
            return super.calculateMemory() + PAGE_NODE_MEMORY +
                    getRawChildPageCount() * (MEMORY_POINTER + PAGE_MEMORY_CHILD);
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append("[").append(Long.toHexString(children[i].pos)).append("]");
                if(i < keyCount) {
                    buff.append(" ").append(getKey(i));
                }
            }
        }
    }


    public static final class PageReference {

        public static final PageReference EMPTY = new PageReference(null, 0, 0);

        final long pos;

        final Page page;

        final long count;

        public PageReference(Page page) {
            this(page, page.getPos(), page.getTotalCount());
        }

        PageReference(long pos, long count) {
            this(null, pos, count);
            assert pos != 0;
        }

        private PageReference(Page page, long pos, long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }

        @Override
        public String toString() {
            return "Cnt:" + count + ", pos:" + DataUtils.getPageChunkId(pos) +
                    "-" + DataUtils.getPageOffset(pos) + ":" + DataUtils.getPageMaxLength(pos) +
                    (DataUtils.getPageType(pos) == 0 ? " leaf" : " node") + ", " + page;
        }
    }

}
