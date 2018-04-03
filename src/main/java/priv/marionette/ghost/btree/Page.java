package priv.marionette.ghost.btree;

import priv.marionette.compress.Compressor;
import priv.marionette.ghost.type.DataType;
import priv.marionette.tools.DataUtils;

import java.nio.ByteBuffer;

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
public class Page {

    /**
     * 空节点拥有同一指针
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private static final int IN_MEMORY = 0x80000000;

    private final MVMap<?, ?> map;

    private long version;

    private long pos;

    private long totalCount;

    private int cachedCompare;

    private int memory;

    private Object[] keys;

    private Object[] values;

    private PageReference[] children;

    private volatile boolean removedInMemory;


    Page(MVMap<?, ?> map, long version) {
        this.map = map;
        this.version = version;
    }

    static Page createEmpty(MVMap<?, ?> map, long version) {
        return create(map, version,
                EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY,
                null,
                0, DataUtils.PAGE_MEMORY);
    }

    public static Page create(MVMap<?, ?> map, long version,
                              Object[] keys, Object[] values, PageReference[] children,
                              long totalCount, int memory) {
        Page p = new Page(map, version);
        // the position is 0
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.totalCount = totalCount;
        BTreeWithMVCC bTree = map.bTree;
        if(bTree.getFileStore() == null) {
            p.memory = IN_MEMORY;
        } else if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        if(bTree.getFileStore() != null) {
            bTree.registerUnsavedPage(p.memory);
        }
        return p;
    }

    private void addMemory(int mem) {
        memory += mem;
    }

    public static class PageReference {

        /**
         * The position, if known, or 0.
         */
        final long pos;

        /**
         * The page, if in memory, or null.
         */
        final Page page;

        /**
         * The descendant count for this child page.
         */
        final long count;

        public PageReference(Page page, long pos, long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }

    }

    private void recalculateMemory() {
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

    long getVersion() {
        return version;
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


    public boolean isLeaf() {
        return children == null;
    }

    public int getRawChildPageCount() {
        return children.length;
    }


    public Page copy(long version) {
        Page newPage = create(map, version,
                keys, values,
                children, totalCount,
                memory);
        // 旧版本逻辑删除
        removePage();
        newPage.cachedCompare = cachedCompare;
        return newPage;
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

    private boolean isPersistent() {
        return memory != IN_MEMORY;
    }

    void setVersion(long version) {
        this.version = version;
    }

    public long getPos() {
        return pos;
    }



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
        Page p = new Page(map, 0);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

    void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
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
        boolean node = (type & 1) == DataUtils.PAGE_TYPE_NODE;
        if (node) {
            children = new PageReference[len + 1];
            long[] p = new long[len + 1];
            for (int i = 0; i <= len; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= len; i++) {
                long s = DataUtils.readVarLong(buff);
                total += s;
                children[i] = new PageReference(null, p[i], s);
            }
            totalCount = total;
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
        if (!node) {
            values = new Object[len];
            map.getValueType().read(buff, values, len, false);
            totalCount = len;
        }
        recalculateMemory();
    }

    public long getTotalCount() {
        if (BTreeWithMVCC.ASSERT) {
            long check = 0;
            if (isLeaf()) {
                check = keys.length;
            } else {
                for (PageReference x : children) {
                    check += x.count;
                }
            }
            if (check != totalCount) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Expected: {0} got: {1}", check, totalCount);
            }
        }
        return totalCount;
    }

    public Object getKey(int index) {
        return keys[index];
    }


    Page split(int at) {
        Page page = isLeaf() ? splitLeaf(at) : splitNode(at);
        if(isPersistent()) {
            recalculateMemory();
        }
        return page;
    }

    private Page splitLeaf(int at) {
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;
        Object[] aValues = new Object[a];
        Object[] bValues = new Object[b];
        bValues = new Object[b];
        System.arraycopy(values, 0, aValues, 0, a);
        System.arraycopy(values, a, bValues, 0, b);
        values = aValues;
        totalCount = a;
        Page newPage = create(map, version,
                bKeys, bValues,
                null,
                b, 0);
        return newPage;
    }


    private Page splitNode(int at) {
        int a = at, b = keys.length - a;

        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b - 1];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a + 1, bKeys, 0, b - 1);
        keys = aKeys;

        PageReference[] aChildren = new PageReference[a + 1];
        PageReference[] bChildren = new PageReference[b];
        System.arraycopy(children, 0, aChildren, 0, a + 1);
        System.arraycopy(children, a + 1, bChildren, 0, b);
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
        Page newPage = create(map, version,
                bKeys, null,
                bChildren,
                t, 0);
        return newPage;
    }


    /**
     * 记忆化二分搜索，将最近的一次搜索结果的index保存起来，
     * 以此应对相同字段的冗余搜索,这样每次搜索的起点并不同
     * 于常规的二分搜索(中点)。所以，所谓的算法是一种思想，
     * 是一种从计算机的角度看世界的方法，而不是现在很多人所
     * 理解的刻板的"模型"，或者解决问题甚至沦为装X的工具，这
     * 样的人写出的代码是没有自己的灵魂的。
     * 希望正在阅读的人可以感悟到作者此刻的心情。
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
    public void insertLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        keys = newKeys;
        Object[] newValues = new Object[len];
        DataUtils.copyWithGap(values, newValues, len - 1, index);
        values = newValues;
        keys[index] = key;
        values[index] = value;
        totalCount++;
        if(isPersistent()) {
            addMemory(map.getKeyType().getMemory(key) +
                    map.getValueType().getMemory(value));
        }
    }

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

    public void setChild(int index, Page c) {
        if (c == null) {
            long oldCount = children[index].count;
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            PageReference ref = new PageReference(null, 0, 0);
            children[index] = ref;
            totalCount -= oldCount;
        } else if (c != children[index].page ||
                c.getPos() != children[index].pos) {
            long oldCount = children[index].count;
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            PageReference ref = new PageReference(c, c.pos, c.totalCount);
            children[index] = ref;
            totalCount += c.totalCount - oldCount;
        }
    }


    public void insertNode(int index, Object key, Page childPage) {

        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;

        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount + 1];
        DataUtils.copyWithGap(children, newChildren, childCount, index);
        newChildren[index] = new PageReference(
                childPage, childPage.getPos(), childPage.totalCount);
        children = newChildren;

        totalCount += childPage.totalCount;
        if(isPersistent()) {
            addMemory(map.getKeyType().getMemory(key) +
                    DataUtils.PAGE_MEMORY_CHILD);
        }
    }

    /**
     * 根据index获取响应的子节点
     * @param index
     * @return
     */
    public Page getChildPage(int index) {
        PageReference ref = children[index];
        return ref.page != null ? ref.page : map.readPage(ref.pos);
    }

    long getCounts(int index) {
        return children[index].count;
    }

    /**
     * 记录多少被当前page引用/间接引用的其他pages的信息，通过引用计数法判断chunks的使用率，
     * 从而进行垃圾回收
     */
    public static  class PageChildren{

    }

}
