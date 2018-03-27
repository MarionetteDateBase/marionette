package priv.marionette.ghost.btree;

import priv.marionette.ghost.type.DataType;
import priv.marionette.tools.DataUtils;

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

    /**
     * 记录多少被当前page引用/间接引用的其他pages的信息，通过引用计数法判断chunks的使用率，
     * 从而进行垃圾回收
     */
    public static  class PageChildren{

    }

}
