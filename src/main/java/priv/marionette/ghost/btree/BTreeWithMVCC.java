package priv.marionette.ghost.btree;

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
    static final int BLOCK_SIZE = 4 * 1024;

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

    private static class BackgroundWriterThread extends Thread {

    }





}
