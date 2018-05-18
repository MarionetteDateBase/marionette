package priv.marionette.ghost;

import priv.marionette.tools.MathUtils;

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
     * 位图
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

    /**
     * 检查从某个offset开始某一长度所包含的区块是否全部正在使用
     * @param pos
     * @param length
     * @return
     */
    public boolean isUsed(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        for (int i = start; i < start + blocks; i++) {
            if (!set.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 检查从某个offset开始某一长度所包含的区块是否全部等待释放
     * @param pos
     * @param length
     * @return
     */
    public boolean isFree(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        for (int i = start; i < start + blocks; i++) {
            if (set.get(i)) {
                return false;
            }
        }
        return true;
    }

    public long allocate(int length) {
        return allocate(length, true);
    }

    public long predictAllocation(int length) {
        return allocate(length, false);
    }

    /**
     * 分配区块空间并标记正在使用
     * @param length
     * @return
     */
    private long allocate(int length, boolean allocate) {
        int blocks = getBlockCount(length);
        for (int i = 0;;) {
            int start = set.nextClearBit(i);
            int end = set.nextSetBit(start + 1);
            if (end < 0 || end - start >= blocks) {
                assert set.nextSetBit(start) == -1 || set.nextSetBit(start) >= start + blocks :
                        "Double alloc: " + Integer.toHexString(start) + "/" + Integer.toHexString(blocks) + " " + this;
                if (allocate) {
                    set.set(start, start + blocks);
                }
                return getPos(start);
            }
            i = end;
        }
    }


    /**
     * 将指定区间强制标记为已使用
     * @param pos
     * @param length
     */
    public void markUsed(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        set.set(start, start + blocks);
    }


    /**
     * 将指定空间强制释放
     * @param pos
     * @param length
     */
    public void free(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        set.clear(start, start + blocks);
    }



    private long getPos(int block) {
        return (long) block * (long) blockSize;
    }


    private int getBlock(long pos) {
        return (int) (pos / blockSize);
    }

    private int getBlockCount(int length) {
        return MathUtils.roundUpInt(length, blockSize) / blockSize;
    }


    /**
     * 计算当前空间的总体使用率
     * @return
     */
    public int getFillRate() {
        int total = set.length(), count = 0;
        for (int i = 0; i < total; i++) {
            if (set.get(i)) {
                count++;
            }
        }
        if (count == 0) {
            return 0;
        }
        return Math.max(1, (int) (100L * count / total));
    }

    /**
     * 获取第一个free空间的offset
     * @return
     */
    public long getFirstFree() {
        return getPos(set.nextClearBit(0));
    }

    /**
     * 获取最后一个free空间的offset
     * @return
     */
    public long getLastFree() {
        return getPos(set.previousSetBit(set.size()-1) + 1);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (DETAILED_INFO) {
            int onCount = 0, offCount = 0;
            int on = 0;
            for (int i = 0; i < set.length(); i++) {
                if (set.get(i)) {
                    onCount++;
                    on++;
                } else {
                    offCount++;
                }
                if ((i & 1023) == 1023) {
                    buff.append(String.format("%3x", on)).append(' ');
                    on = 0;
                }
            }
            buff.append('\n')
                    .append(" on ").append(onCount).append(" off ").append(offCount)
                    .append(' ').append(100 * onCount / (onCount+offCount)).append("% used ");
        }
        buff.append('[');
        for (int i = 0;;) {
            if (i > 0) {
                buff.append(", ");
            }
            int start = set.nextClearBit(i);
            buff.append(Integer.toHexString(start)).append('-');
            int end = set.nextSetBit(start + 1);
            if (end < 0) {
                break;
            }
            buff.append(Integer.toHexString(end - 1));
            i = end + 1;
        }
        buff.append(']');
        return buff.toString();
    }




}
