package priv.marionette.ghost;

import priv.marionette.tools.DataUtils;

import java.nio.ByteBuffer;

/**
 * 根据Write类型自动扩展ByteBuffer
 *
 * @author Yue Yu
 * @create 2018-03-13 上午11:41
 **/
public class WriteBuffer {
    /**
     * 初始ByteBuffer最大值(4KB)
     */
    private static final int MAX_REUSE_CAPACITY = 4 * 1024 * 1024;

    /**
     * ByteBuffer最小增长率(1KB)
     */
    private static final int MIN_GROW = 1024 * 1024;

    /**
     * clear之后重新定义一个buffer来使用，双桶置换策略提高内存读写效率，降低ByteBuffer新建损耗
     */
    private ByteBuffer reuse;

    /**
     * 当前使用的buffer
     */
    private ByteBuffer buff;

    public WriteBuffer(int initialSize) {
        reuse = ByteBuffer.allocate(initialSize);
        buff = reuse;
    }

    public WriteBuffer() {
        this(MIN_GROW);
    }

    /**
     * Write a VarInt
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putVarInt(int x) {
        DataUtils.writeVarInt(ensureCapacity(5), x);
        return this;
    }

    /**
     * Write a VarLong
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putVarLong(long x) {
        DataUtils.writeVarLong(ensureCapacity(10), x);
        return this;
    }

    /**
     * 可兼容UTF-8的形式(3个字节)来写字符串
     *
     * @param s the string
     * @param len the number of characters to write
     * @return this
     */
    public WriteBuffer putStringData(String s, int len) {
        ByteBuffer b = ensureCapacity(3 * len);
        DataUtils.writeStringData(b, s, len);
        return this;
    }

    /**
     * Put a byte.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer put(byte x) {
        ensureCapacity(1).put(x);
        return this;
    }

    /**
     * Put a character.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putChar(char x) {
        ensureCapacity(2).putChar(x);
        return this;
    }

    /**
     * Put a short.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putShort(short x) {
        ensureCapacity(2).putShort(x);
        return this;
    }

    /**
     * Put an integer.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putInt(int x) {
        ensureCapacity(4).putInt(x);
        return this;
    }

    /**
     * Put a long.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putLong(long x) {
        ensureCapacity(8).putLong(x);
        return this;
    }

    /**
     * Put a float.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putFloat(float x) {
        ensureCapacity(4).putFloat(x);
        return this;
    }

    /**
     * Put a double.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putDouble(double x) {
        ensureCapacity(8).putDouble(x);
        return this;
    }

    /**
     * Put a byte array.
     *
     * @param bytes the value
     * @return this
     */
    public WriteBuffer put(byte[] bytes) {
        ensureCapacity(bytes.length).put(bytes);
        return this;
    }

    /**
     * Put a byte array.
     *
     * @param bytes the value
     * @param offset the source offset
     * @param length the number of bytes
     * @return this
     */
    public WriteBuffer put(byte[] bytes, int offset, int length) {
        ensureCapacity(length).put(bytes, offset, length);
        return this;
    }

    /**
     * Put the contents of a byte buffer.
     *
     * @param src the source buffer
     * @return this
     */
    public WriteBuffer put(ByteBuffer src) {
        ensureCapacity(src.remaining()).put(src);
        return this;
    }

    /**
     * 设置ByteBuffer的limit参数
     *
     * @param newLimit the new limit
     * @return this
     */
    public WriteBuffer limit(int newLimit) {
        ensureCapacity(newLimit - buff.position()).limit(newLimit);
        return this;
    }

    /**
     * 获取buffer大小
     *
     * @return the capacity
     */
    public int capacity() {
        return buff.capacity();
    }

    /**
     * 设置ByteBuffer的position参数
     *
     * @param newPosition the new position
     * @return the new position
     */
    public WriteBuffer position(int newPosition) {
        buff.position(newPosition);
        return this;
    }

    /**
     * 获取ByteBuffer的limit参数
     *
     * @return the limit
     */
    public int limit() {
        return buff.limit();
    }

    /**
     * 获取ByteBuffer的position参数
     *
     * @return the position
     */
    public int position() {
        return buff.position();
    }

    /**
     * 从offset为0的位置开始替换ByteBuffer里的字节为指定ByteArray里的字节
     *
     * @param dst the destination array
     * @return this
     */
    public WriteBuffer get(byte[] dst) {
        buff.get(dst);
        return this;
    }

    /**
     * 替换指定Index的Value为一个Int Value
     *
     * @param index the index
     * @param value the value
     * @return this
     */
    public WriteBuffer putInt(int index, int value) {
        buff.putInt(index, value);
        return this;
    }

    /**
     * 替换指定Index的Value为一个Short Value
     *
     * @param index the index
     * @param value the value
     * @return this
     */
    public WriteBuffer putShort(int index, short value) {
        buff.putShort(index, value);
        return this;
    }

    /**
     * clear the buffer，并置为reuse所引用的buffer
     *
     * @return this
     */
    public WriteBuffer clear() {
        if (buff.limit() > MAX_REUSE_CAPACITY) {
            buff = reuse;
        } else if (buff != reuse) {
            reuse = buff;
        }
        buff.clear();
        return this;
    }

    /**
     * 获取当前使用的buffer
     *
     * @return the byte buffer
     */
    public ByteBuffer getBuffer() {
        return buff;
    }

    private ByteBuffer ensureCapacity(int len) {
        if (buff.remaining() < len) {
            grow(len);
        }
        return buff;
    }

    private void grow(int additional) {
        ByteBuffer temp = buff;
        int needed = additional - temp.remaining();
        long grow = Math.max(needed, MIN_GROW);
        // 至少增长当前buffer size的50%
        grow = Math.max(temp.capacity() / 2, grow);
        // java的数组大小不可超越(2^32-1)
        int newCapacity = (int) Math.min(Integer.MAX_VALUE, temp.capacity() + grow);
        if (newCapacity < needed) {
            throw new OutOfMemoryError("Capacity: " + newCapacity + " needed: " + needed);
        }
        try {
            buff = ByteBuffer.allocate(newCapacity);
        } catch (OutOfMemoryError e) {
            throw new OutOfMemoryError("Capacity: " + newCapacity);
        }
        temp.flip();
        buff.put(temp);
        if (newCapacity <= MAX_REUSE_CAPACITY) {
            reuse = buff;
        }
    }

}
