package priv.marionette.ghost.btree;

import priv.marionette.ghost.type.DataType;
import priv.marionette.tools.ConcurrentArrayList;
import priv.marionette.tools.DataUtils;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

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
public class MVMap<K,V> extends AbstractMap<K, V>
        implements ConcurrentMap<K, V> {

    protected BTreeWithMVCC bTree;


    /**
     * 当前BTree的根结点
     */
    protected volatile Page root;

    /**
     * 当前分支版本
     */
    protected volatile long writeVersion;

    private int id;
    private long createVersion;
    private final DataType keyType;
    private final DataType valueType;

    private final ConcurrentArrayList<Page> oldRoots =
            new ConcurrentArrayList<>();

    /**
     * 跨越内存栅栏时强制同步map的开启状态，避免写入一个已关闭的文件流
     */
    private volatile boolean closed;
    private boolean readOnly;
    private boolean isVolatile;

    protected MVMap(DataType keyType, DataType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    static String getMapRootKey(int mapId) {
        return "root." + Integer.toHexString(mapId);
    }

    static String getMapKey(int mapId) {
        return "map." + Integer.toHexString(mapId);
    }

    protected void init(BTreeWithMVCC bTree, HashMap<String, Object> config) {
        this.bTree = bTree;
        this.id = DataUtils.readHexInt(config, "id", 0);
        this.createVersion = DataUtils.readHexLong(config, "createVersion", 0);
        this.writeVersion = bTree.getCurrentVersion();
        this.root = Page.createEmpty(this,  -1);
    }



    @Override
    @SuppressWarnings("unchecked")
    public synchronized V put(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        beforeWrite();
        long v = writeVersion;
        //写时复制
        Page p = root.copy(v);
        //判断节点是否需要分裂至半满
        p = splitRootIfNeeded(p, v);
        Object result = put(p, v, key, value);
        newRoot(p);
        return (V) result;
    }

    protected Object put(Page p, long writeVersion, Object key, Object value) {
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index < 0) {
                index = -index - 1;
                p.insertLeaf(index, key, value);
                return null;
            }
            return p.setValue(index, value);
        }
        // p是内部节点的情况下
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        //如果是内部节点，那么它的key对应的子节点也要用同一版本号复制一份
        Page c = p.getChildPage(index).copy(writeVersion);
        if (c.getMemory() > bTree.getPageSplitSize() && c.getKeyCount() > 1) {
            // 自顶向下分裂
            int at = c.getKeyCount() / 2;
            Object k = c.getKey(at);
            Page split = c.split(at);
            p.setChild(index, split);
            p.insertNode(index, k, c);
            // 递归调用直到达到叶子节点
            return put(p, writeVersion, key, value);
        }
        Object result = put(c, writeVersion, key, value);
        p.setChild(index, c);
        return result;
    }


    int compare(Object a, Object b) {
        return keyType.compare(a, b);
    }

    public int getId() {
        return id;
    }

    public BTreeWithMVCC getBTree() {
        return bTree;
    }

    protected Page splitRootIfNeeded(Page p, long writeVersion) {
        if (p.getMemory() <= bTree.getPageSplitSize() || p.getKeyCount() <= 1) {
            return p;
        }
        int at = p.getKeyCount() / 2;
        long totalCount = p.getTotalCount();
        Object k = p.getKey(at);
        Page split = p.split(at);
        Object[] keys = { k };
        Page.PageReference[] children = {
                new Page.PageReference(p, p.getPos(), p.getTotalCount()),
                new Page.PageReference(split, split.getPos(), split.getTotalCount()),
        };
        p = Page.create(this, writeVersion,
                keys, null,
                children,
                totalCount, 0);
        return p;
    }

    protected void newRoot(Page newRoot) {
        if (root != newRoot) {
            removeUnusedOldVersions();
            if (root.getVersion() != newRoot.getVersion()) {
                Page last = oldRoots.peekLast();
                if (last == null || last.getVersion() != root.getVersion()) {
                    oldRoots.add(root);
                }
            }
            root = newRoot;
        }
    }

    void removeUnusedOldVersions() {
        long oldest = bTree.getOldestVersionToKeep();
        if (oldest == -1) {
            return;
        }
        Page last = oldRoots.peekLast();
        while (true) {
            Page p = oldRoots.peekFirst();
            if (p == null || p.getVersion() >= oldest || p == last) {
                break;
            }
            oldRoots.removeFirst(p);
        }
    }



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

    public long getVersion() {
        return root.getVersion();
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

    void setWriteVersion(long writeVersion) {
        this.writeVersion = writeVersion;
    }

    void setRootPos(long rootPos, long version) {
        root = rootPos == 0 ? Page.createEmpty(this, -1) : readPage(rootPos);
        root.setVersion(version);
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
                return MVMap.this.size();
            }

            @Override
            @SuppressWarnings("unchecked")
            public int indexOf(Object key) {
                return (int) getKeyIndex((K) key);
            }

        };
    }


    public K getKey(long index) {
        if (index < 0 || index >= size()) {
            return null;
        }
        Page p = root;
        long offset = 0;
        while (true) {
            if (p.isLeaf()) {
                if (index >= offset + p.getKeyCount()) {
                    return null;
                }
                return (K) p.getKey((int) (index - offset));
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

    public long getKeyIndex(K key) {
        if (size() == 0) {
            return -1;
        }
        Page p = root;
        long offset = 0;
        while (true) {
            int x = p.binarySearch(key);
            if (p.isLeaf()) {
                if (x < 0) {
                    return -offset + x;
                }
                return offset + x;
            }
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
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







    protected int getChildPageCount(Page p) {
        return p.getRawChildPageCount();
    }







}
