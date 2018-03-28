package priv.marionette.ghost.btree;

import priv.marionette.ghost.type.DataType;
import priv.marionette.tools.ConcurrentArrayList;
import priv.marionette.tools.DataUtils;

import java.util.AbstractMap;
import java.util.HashMap;
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
 * 以上策略常被人总结为：Copy On Write，用以提高并发性，常见应用有redis
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










}
