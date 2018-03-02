package priv.marionette.cache;

import priv.marionette.tools.DataUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * LRIS页面置换策略缓存,运用了分代策略的高性能缓存
 * 由张晓东和Song Jiang发明，http://www.cse.ohio-state.edu/~zhang/lirs-sigmetrics-02.html
 * 现将其改良，细化entry存放介质的粒度,提高并发性（思路参考concurrentHashMap的实现）
 *
 * @author Yue Yu
 * @create 2018-01-05 下午3:10
 **/
public class LIRSCache<V> {

    private long maxMemory;

    private final Segment<V>[] segments;

    private final int segmentCount;

    //段偏移量
    private final int segmentShift;

    //段掩码
    private final int segmentMask;

    private final int stackMoveDistance;

    private final int nonResidentQueueSize;

    /**
     * 计算key的hash值
     *
     * @param key the key
     * @return the hash code
     */
    static int getHash(long key) {
        //先得到初始的hash
        int hash = (int) ((key >>> 32) ^ key);
        // 根据h再去做hash，让hash的值能均匀shard到各个segment上面
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }

    /**
     * 缓存初始化
     *
     * @param config 缓存配置
     */
    @SuppressWarnings("unchecked")
    public LIRSCache(Config config) {
        setMaxMemory(config.maxMemory);
        this.nonResidentQueueSize = config.nonResidentQueueSize;
        DataUtils.checkArgument(
                Integer.bitCount(config.segmentCount) == 1,
                "The segment count must be a power of 2, is {0}", config.segmentCount);
        this.segmentCount = config.segmentCount;
        this.segmentMask = segmentCount - 1;
        this.stackMoveDistance = config.stackMoveDistance;
        segments = new Segment[segmentCount];
        clear();
        this.segmentShift = 32 - Integer.bitCount(segmentMask);
    }

    /**
     * 计算resident entries的数量.
     *
     * @return the number of entries
     */
    public int size() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.mapSize - s.queue2Size;
        }
        return x;
    }

    /**
     * 获取用于存储value的空间，默认为1
     *
     * @param value the value
     * @return the size
     */
    @SuppressWarnings("unused")
    protected int sizeOf(V value) {
        return 1;
    }

    /**
     * 通过value的大小设置存储用空间
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value, or null if there was no resident entry
     */
    public V put(long key, V value) {
        return put(key, value, sizeOf(value));
    }

    /**
     *往缓存里添加一个entry，如果这个元素原来就在缓存里，那么将其置于热区栈顶，
     *如果原来不在缓存里，那么置于冷区栈顶
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @param memory the memory used for the given entry
     * @return the old value, or null if there was no resident entry
     */
    public V put(long key, V value, int memory) {
        int hash = getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> s = segments[segmentIndex];
        // check whether resize is required: synchronize on s, to avoid
        // concurrent resizes (concurrent reads read
        // from the old segment)
        synchronized (s) {
            s = resizeIfNeeded(s, segmentIndex);
            return s.put(key, hash, value, memory);
        }
    }

    /**
     * 查找缓存是是否包含指定的key，不会改变缓存状态
     *
     * @param key the key (may not be null)
     * @return true if there is a resident entry
     */
    public boolean containsKey(long key) {
        int hash = getHash(key);
        return getSegment(hash).containsKey(key, hash);
    }

    /**
     * 获取指定key的value，但是不会改变缓存状态
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V peek(long key) {
        Entry<V> e = find(key);
        return e == null ? null : e.value;
    }

    private Entry<V> find(long key) {
        int hash = getHash(key);
        return getSegment(hash).find(key, hash);
    }


    private Segment<V> getSegment(int hash) {
        return segments[getSegmentIndex(hash)];
    }

    /**
     * 根据key获取缓存中的value，并改变缓存状态
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V get(long key) {
        int hash = getHash(key);
        return getSegment(hash).get(key, hash);
    }


    /**
     * 在定位后判断map是否需要扩容
     *
     */
    private Segment<V> resizeIfNeeded(Segment<V> s, int segmentIndex) {
        int newLen = s.getNewMapLen();
        if (newLen == 0) {
            return s;
        }
        // another thread might have resized
        // (as we retrieved the segment before synchronizing on it)
        Segment<V> s2 = segments[segmentIndex];
        if (s == s2) {
            // no other thread resized, so we do
            s = new Segment<>(s, newLen);
            segments[segmentIndex] = s;
        }
        return s;
    }

    private int getSegmentIndex(int hash) {
        return (hash >>> segmentShift) & segmentMask;
    }



    public void clear() {
        long max = getMaxItemSize();
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment<>(
                    max, stackMoveDistance, 8, nonResidentQueueSize);
        }
    }

    /**
     * 移除指定entry(resident or non-resident)
     *
     * @param key the key (may not be null)
     * @return the old value, or null if there was no resident entry
     */
    public V remove(long key) {
        int hash = getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> s = segments[segmentIndex];
        // check whether resize is required: synchronize on s, to avoid
        // concurrent resizes (concurrent reads read
        // from the old segment)
        synchronized (s) {
            s = resizeIfNeeded(s, segmentIndex);
            return s.remove(key, hash);
        }
    }

    public long getMaxItemSize() {
        return Math.max(1, maxMemory / segmentCount);
    }

    public void setMaxMemory(long maxMemory) {
        DataUtils.checkArgument(
                maxMemory > 0,
                "Max memory must be larger than 0, is {0}", maxMemory);
        this.maxMemory = maxMemory;
        if (segments != null) {
            long max = 1 + maxMemory / segments.length;
            for (Segment<V> s : segments) {
                s.setMaxMemory(max);
            }
        }
    }



    /**
     * 将缓存细化为Segment粒度
     *
     * @param <V> the value type
     */
    private static class Segment<V> {

        /**
         * map里面的所有entry（hot，cold，non-resident）
         */
        int mapSize;

        /**
         * 队列中的resident cold entries的数量
         */
        int queueSize;

        /**
         * 队列中的non-resident cold entries的数量
         */
        int queue2Size;

        /**
         * 缓存hits数量
         */
        long hits;

        /**
         * 缓存misses数量
         */
        long misses;

        /**
         * map数组，size是2的N次方
         */
        final Entry<V>[] entries;

        /**
         * 当前已用内存大小
         */
        long usedMemory;

        /**
         * 在当前entry置顶前有多少entry置顶
         */
        private final int stackMoveDistance;

        /**
         * 此段最多使用的内存大小（字节为单位）
         */
        private long maxMemory;

        /**
         * 二进制掩码，作用与段掩码相同，通过hash值定位map中的元素位置
         */
        private final int mask;

        /**
         * map中允许存在的non-resident entry数量的最大值
         *
         */
        private final int nonResidentQueueSize;

        /**
         * 存储最近被引用的entries,里面包含hot entries、部分最近引用的resident cold entries（至少有一个头节点）
         *
         */
        private final Entry<V> stack;

        /**
         * 热区entries的数量
         */
        private int stackSize;

        /**
         * 存储resident cold entries 的队列（至少有一个头节点）
         */
        private final Entry<V> queue;

        /**
         * 存储non-resident cold entries 的队列（至少有一个头节点）
         */
        private final Entry<V> queue2;

        /**
         * 移至栈顶的总数
         */
        private int stackMoveCounter;


        /**
         * 创建一个cache segment.
         *
         * @param maxMemory the maximum memory to use
         * @param stackMoveDistance the number of other entries to be moved to
         *        the top of the stack before moving an entry to the top
         * @param len the number of hash table buckets (must be a power of 2)
         * @param nonResidentQueueSize the non-resident queue size factor
         */
        Segment(long maxMemory, int stackMoveDistance, int len,
                int nonResidentQueueSize) {
            setMaxMemory(maxMemory);
            this.stackMoveDistance = stackMoveDistance;
            this.nonResidentQueueSize = nonResidentQueueSize;

            mask = len - 1;

            stack = new Entry<>();
            stack.stackPrev = stack.stackNext = stack;
            queue = new Entry<>();
            queue.queuePrev = queue.queueNext = queue;
            queue2 = new Entry<>();
            queue2.queuePrev = queue2.queueNext = queue2;

            @SuppressWarnings("unchecked")
            Entry<V>[] e = new Entry[len];
            entries = e;
        }

        /**
         * 基于原来的segment扩容（缩减）,在resize的时候加锁
         *
         * @param old the old segment
         * @param len the number of hash table buckets (must be a power of 2)
         */
        Segment(Segment<V> old, int len) {
            this(old.maxMemory, old.stackMoveDistance, len, old.nonResidentQueueSize);
            hits = old.hits;
            misses = old.misses;
            Entry<V> s = old.stack.stackPrev;
            while (s != old.stack) {
                Entry<V> e = copy(s);
                addToMap(e);
                addToStack(e);
                s = s.stackPrev;
            }
            s = old.queue.queuePrev;
            while (s != old.queue) {
                Entry<V> e = find(s.key, getHash(s.key));
                if (e == null) {
                    e = copy(s);
                    addToMap(e);
                }
                addToQueue(queue, e);
                s = s.queuePrev;
            }
            s = old.queue2.queuePrev;
            while (s != old.queue2) {
                Entry<V> e = find(s.key, getHash(s.key));
                if (e == null) {
                    e = copy(s);
                    addToMap(e);
                }
                addToQueue(queue2, e);
                s = s.queuePrev;
            }
        }

        /**
         * 计算map需要扩容（缩减）多少
         *
         * @return 0 if no resizing is needed, or the new length
         */
        int getNewMapLen() {
            int len = mask + 1;
            if (len * 3 < mapSize * 4 && len < (1 << 28)) {
                // 如果当前使用率超过75%
                return len * 2;
            } else if (len > 32 && len / 8 > mapSize) {
                // 如果当前使用率小于12%
                return len / 2;
            }
            return 0;
        }

        private void addToMap(Entry<V> e) {
            int index = getHash(e.key) & mask;
            e.mapNext = entries[index];
            entries[index] = e;
            usedMemory += e.memory;
            mapSize++;
        }

        private static <V> Entry<V> copy(Entry<V> old) {
            Entry<V> e = new Entry<>();
            e.key = old.key;
            e.value = old.value;
            e.memory = old.memory;
            e.topMove = old.topMove;
            return e;
        }

        /**
         * 查询当前key所属的entry占用多少内存
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return the memory, or 0 if there is no resident entry
         */
        int getMemory(long key, int hash) {
            Entry<V> e = find(key, hash);
            return e == null ? 0 : e.memory;
        }

        /**
         * 根据key取得对应得value并改变缓存状态
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return the value, or null if there is no resident entry
         */
        V get(long key, int hash) {
            Entry<V> e = find(key, hash);
            if (e == null) {
                // the entry was not found
                misses++;
                return null;
            }
            V value = e.value;
            if (value == null) {
                // it was a non-resident entry
                misses++;
                return null;
            }
            if (e.isHot()) {
                if (e != stack.stackNext) {
                    if (stackMoveDistance == 0 ||
                            stackMoveCounter - e.topMove > stackMoveDistance) {
                        access(key, hash);
                    }
                }
            } else {
                access(key, hash);
            }
            hits++;
            return value;
        }

        /**
         * 将要被get的entry提到热区栈/冷区队列顶部
         *
         * @param key the key
         */
        private synchronized void access(long key, int hash) {
            Entry<V> e = find(key, hash);
            if (e == null || e.value == null) {
                return;
            }
            if (e.isHot()) {
                if (e != stack.stackNext) {
                    if (stackMoveDistance == 0 ||
                            stackMoveCounter - e.topMove > stackMoveDistance) {
                        //将entry置于栈顶
                        boolean wasEnd = e == stack.stackPrev;
                        removeFromStack(e);
                        if (wasEnd) {
                            //如果末尾的entry为cold，置为hot
                            pruneStack();
                        }
                        addToStack(e);
                    }
                }
            } else {
                //从此不在是cold entry，农民翻身做主人
                removeFromQueue(e);
                if (e.stackNext != null) {
                    // 将原来的entry移除，因为之后会添加新的entry进栈顶
                    removeFromStack(e);
                    //cold区提上来一个entry，故下方热区最末尾的entry
                    convertOldestHotToCold();
                } else {
                    //如果不在，那么优先淘汰它
                    addToQueue(queue, e);
                }
                // 被access的entry置于栈顶
                addToStack(e);
            }
        }

        /**
         * 往缓存里添加一个entry，如果这个元素原来就在缓存里，那么将其置于热区栈顶，
         * 如果原来不在缓存里，那么置于冷区栈顶
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @param value the value (may not be null)
         * @param memory the memory used for the given entry
         * @return the old value, or null if there was no resident entry
         */
        synchronized V put(long key, int hash, V value, int memory) {
            if (value == null) {
                throw DataUtils.newIllegalArgumentException(
                        "The value may not be null");
            }
            V old;
            Entry<V> e = find(key, hash);
            boolean existed;
            if (e == null) {
                existed = false;
                old = null;
            } else {
                existed = true;
                old = e.value;
                remove(key, hash);
            }
            if (memory > maxMemory) {
                // 如果使用的内存超出内存限制，则失败
                return old;
            }
            e = new Entry<>();
            e.key = key;
            e.value = value;
            e.memory = memory;
            int index = hash & mask;
            e.mapNext = entries[index];
            entries[index] = e;
            usedMemory += memory;
            if (usedMemory > maxMemory) {
                // 释放热缓存
                evict();
                if (stackSize > 0) {
                    // 加入队列顶部，要淘汰将会优先淘汰它
                    addToQueue(queue, e);
                }
            }
            mapSize++;
            // 在栈上置顶新加入的元素
            addToStack(e);
            if (existed) {
                // 如果之前这个entry已经在栈上，则状态置为hot
                access(key, hash);
            }
            return old;
        }

        /**
         * 移除一个entry
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return the old value, or null if there was no resident entry
         */
        synchronized V remove(long key, int hash) {
            int index = hash & mask;
            Entry<V> e = entries[index];
            if (e == null) {
                return null;
            }
            V old;
            if (e.key == key) {
                old = e.value;
                entries[index] = e.mapNext;
            } else {
                Entry<V> last;
                do {
                    last = e;
                    e = e.mapNext;
                    if (e == null) {
                        return null;
                    }
                } while (e.key != key);
                old = e.value;
                last.mapNext = e.mapNext;
            }
            mapSize--;
            usedMemory -= e.memory;
            if (e.stackNext != null) {
                removeFromStack(e);
            }
            if (e.isHot()) {
                // 当移除一个hot entry，从队列里拿根据LRU策略拿出一个cold entry，将其变为hot entry
                // 并从队列顶移除，移入热区栈尾
                e = queue.queueNext;
                if (e != queue) {
                    removeFromQueue(e);
                    if (e.stackNext == null) {
                        addToStackBottom(e);
                    }
                }
            } else {
                removeFromQueue(e);
            }
            pruneStack();
            return old;
        }



        /**
         * 冷却缓存
         */
        private void evict() {
            do {
                evictBlock();
            } while (usedMemory > maxMemory);
        }

        private void evictBlock() {
            while (queueSize <= (mapSize >>> 5) && stackSize > 0) {
                convertOldestHotToCold();
            }
            //将队列顶部resident cold entry变为non-resident cold entry
            while (usedMemory > maxMemory && queueSize > 0) {
                Entry<V> e = queue.queuePrev;
                usedMemory -= e.memory;
                removeFromQueue(e);
                e.value = null;
                e.memory = 0;
                addToQueue(queue2, e);
                // 清楚过量non-resident cold entry
                int maxQueue2Size = nonResidentQueueSize * (mapSize - queue2Size);
                if (maxQueue2Size >= 0) {
                    while (queue2Size > maxQueue2Size) {
                        e = queue2.queuePrev;
                        int hash = getHash(e.key);
                        remove(e.key, hash);
                    }
                }
            }
        }

        private void convertOldestHotToCold() {
            Entry<V> last = stack.stackPrev;
            if (last == stack) {
                throw new IllegalStateException();
            }
            removeFromStack(last);
            addToQueue(queue, last);
            pruneStack();
        }

        /**
         * 栈尾entry不可为cold
         */
        private void pruneStack() {
            while (true) {
                Entry<V> last = stack.stackPrev;
                if (last.isHot()) {
                    break;
                }
                removeFromStack(last);
            }
        }

        /**
         * 从map中查找值
         *
         * @param key the key
         * @param hash the hash
         * @return the entry (might be a non-resident)
         */
        Entry<V> find(long key, int hash) {
            int index = hash & mask;
            Entry<V> e = entries[index];
            while (e != null && e.key != key) {
                e = e.mapNext;
            }
            return e;
        }

        private void addToStack(Entry<V> e) {
            e.stackPrev = stack;
            e.stackNext = stack.stackNext;
            e.stackNext.stackPrev = e;
            stack.stackNext = e;
            stackSize++;
            e.topMove = stackMoveCounter++;
        }

        private void addToStackBottom(Entry<V> e) {
            e.stackNext = stack;
            e.stackPrev = stack.stackPrev;
            e.stackPrev.stackNext = e;
            stack.stackPrev = e;
            stackSize++;
        }

        /**
         * 将entry从栈内移除（注意，此entry不可为头节点）
         *
         * @param e the entry
         */
        private void removeFromStack(Entry<V> e) {
            e.stackPrev.stackNext = e.stackNext;
            e.stackNext.stackPrev = e.stackPrev;
            e.stackPrev = e.stackNext = null;
            stackSize--;
        }

        private void addToQueue(Entry<V> q, Entry<V> e) {
            e.queuePrev = q;
            e.queueNext = q.queueNext;
            e.queueNext.queuePrev = e;
            q.queueNext = e;
            if (e.value != null) {
                queueSize++;
            } else {
                queue2Size++;
            }
        }

        private void removeFromQueue(Entry<V> e) {
            e.queuePrev.queueNext = e.queueNext;
            e.queueNext.queuePrev = e.queuePrev;
            e.queuePrev = e.queueNext = null;
            if (e.value != null) {
                queueSize--;
            } else {
                queue2Size--;
            }
        }

        /**
         * 批量获取指定keys对应的values
         *
         * @param cold if true, only keys for the cold entries are returned
         * @param nonResident true for non-resident entries
         * @return the key list
         */
        synchronized List<Long> keys(boolean cold, boolean nonResident) {
            ArrayList<Long> keys = new ArrayList<>();
            if (cold) {
                Entry<V> start = nonResident ? queue2 : queue;
                for (Entry<V> e = start.queueNext; e != start;
                     e = e.queueNext) {
                    keys.add(e.key);
                }
            } else {
                for (Entry<V> e = stack.stackNext; e != stack;
                     e = e.stackNext) {
                    keys.add(e.key);
                }
            }
            return keys;
        }

        /**
         * 检测是否存在指定key的resident entry
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return true if there is a resident entry
         */
        boolean containsKey(long key, int hash) {
            Entry<V> e = find(key, hash);
            return e != null && e.value != null;
        }

        /**
         * 遍历获得所有resident entry中的keys
         *
         * @return the set of keys
         */
        synchronized Set<Long> keySet() {
            HashSet<Long> set = new HashSet<>();
            for (Entry<V> e = stack.stackNext; e != stack; e = e.stackNext) {
                set.add(e.key);
            }
            for (Entry<V> e = queue.queueNext; e != queue; e = e.queueNext) {
                set.add(e.key);
            }
            return set;
        }

        void setMaxMemory(long maxMemory) {
            this.maxMemory = maxMemory;
        }


    }


    /**
     * map中的entry模型，其中高频entry（论文中称之为Low IRR）存放于stack中，stack中的entry必须满足低于Recency最大值，这个由LRU策略来保证，
     * 低频entry（High IRR）存放于stack或queue中，而non-resident entry没有具体value，用于计算IRR，故既存在于stack又存在于non-resident queue中，
     * 在淘汰entry优先淘汰它们。
     *
     * @param <V> the value type
     */
    static class Entry<V> {

        /**
         * The key
         */
        long key;

        /**
         * The value
         */
        V value;

        /**
         * entry所占的内存
         */
        int memory;

        /**
         * 是否位于栈顶
         */
        int topMove;

        /**
         * next entry in the stack
         */
        Entry<V> stackNext;

        /**
         * prev entry in the stack
         */
        Entry<V> stackPrev;

        /**
         * next entry in the queue
         */
        Entry<V> queueNext;

        /**
         * prev entry in the queue
         */
        Entry<V> queuePrev;

        /**
         * next entry in the map
         */
        Entry<V> mapNext;

        boolean isHot() {
            return queueNext == null;
        }

    }

    public static class Config {

        /**
         *  缓存最多可使用的内存大小（字节为单位）
         */
        public long maxMemory = 1;

        /**
         * 缓存段数
         */
        public int segmentCount = 16;

        /**
         * 当前元素置顶前已经有多少元素被置顶
         */
        public int stackMoveDistance = 32;

        /**
         * non-resident queue的大小限制
         */
        public final int nonResidentQueueSize = 3;

    }

}


