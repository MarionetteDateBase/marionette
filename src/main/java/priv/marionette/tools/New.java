package priv.marionette.tools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * 静态方法创建常用集合框架
 *
 * @author Yue Yu
 * @create 2018-01-08 下午4:04
 **/
public class New {

    /**
     * 创建动态数组
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> ArrayList<T> arrayList() {
        return new ArrayList<>(4);
    }

    /**
     * 创建哈希字典
     *
     * @param <K> the key type
     * @param <V> the value type
     * @return the object
     */
    public static <K, V> HashMap<K, V> hashMap() {
        return new HashMap<>();
    }

    /**
     * 创建哈希字典
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> HashMap<K, V> hashMap(int initialCapacity) {
        return new HashMap<>(initialCapacity);
    }

    /**
     * 创建哈希集合
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> HashSet<T> hashSet() {
        return new HashSet<>();
    }

    /**
     * 创建动态数组
     *
     * @param <T> the type
     * @param c the collection
     * @return the object
     */
    public static <T> ArrayList<T> arrayList(Collection<T> c) {
        return new ArrayList<>(c);
    }

    /**
     * 创建动态数组
     *
     * @param <T> the type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <T> ArrayList<T> arrayList(int initialCapacity) {
        return new ArrayList<>(initialCapacity);
    }

}
