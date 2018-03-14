package priv.marionette.ghost.type;

import priv.marionette.ghost.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * 数据类型tag
 *
 * @author Yue Yu
 * @create 2018-03-13 下午4:48
 **/
public interface DataType {

    /**
     * compare函数，大小比较
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if larger, and 0 if equal
     * @throws UnsupportedOperationException if the type is not orderable
     */
    int compare(Object a, Object b);

    /**
     * 估算已使用的内存
     *
     * @param obj the object
     * @return the used memory
     */
    int getMemory(Object obj);

    /**
     * 写入一个Object类型字节流
     *
     * @param buff the target buffer
     * @param obj the value
     */
    void write(WriteBuffer buff, Object obj);

    /**
     * 批量写入Object类型字节流
     *
     * @param buff the target buffer
     * @param obj the objects
     * @param len the number of objects to write
     * @param key whether the objects are keys
     */
    void write(WriteBuffer buff, Object[] obj, int len, boolean key);

    /**
     * 读取一个Object类型字节流
     *
     * @param buff the source buffer
     * @return the object
     */
    Object read(ByteBuffer buff);

    /**
     * 批量读取Object类型字节流
     *
     * @param buff the target buffer
     * @param obj the objects
     * @param len the number of objects to read
     * @param key whether the objects are keys
     */
    void read(ByteBuffer buff, Object[] obj, int len, boolean key);

}

