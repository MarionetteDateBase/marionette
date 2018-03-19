package priv.marionette.ghost;

import java.util.BitSet;

/**
 * 利用位图运算标记free space，用以垃圾回收释放空间
 *
 * @author Yue Yu
 * @create 2018-03-13 下午4:40
 **/
public class FreeSpaceBitSet {
    private static final boolean DETAILED_INFO = false;

    /**
     * 第一个有效数据块
     */
    private final int firstFreeBlock;

    private final int blockSize;

    /**
     * The bit set.
     */
    private final BitSet set = new BitSet();
    public FreeSpaceBitSet(int firstFreeBlock, int blockSize){
        this.firstFreeBlock = firstFreeBlock;
        this.blockSize = blockSize;
        clear();
    }

    public void clear() {
        set.clear();
        set.set(0, firstFreeBlock);
    }
}
