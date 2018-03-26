package priv.marionette.tools;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 并发安全型自动扩容数组
 *
 * @author Yue Yu
 * @create 2018-03-26 下午3:38
 **/
public class ConcurrentArrayList<K> {

    @SuppressWarnings("unchecked")
    K[] array = (K[]) new Object[0];


    public K peekFirst() {
        K[] a = array;
        return a.length == 0 ? null : a[0];
    }

    public K peekLast() {
        K[] a = array;
        int len = a.length;
        return len == 0 ? null : a[len - 1];
    }


    public synchronized void add(K obj) {
        if (obj == null) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "adding null value to list");
        }
        int len = array.length;
        array = Arrays.copyOf(array, len + 1);
        array[len] = obj;
    }


    public synchronized boolean removeFirst(K obj) {
        if (peekFirst() != obj) {
            return false;
        }
        int len = array.length;
        @SuppressWarnings("unchecked")
        K[] a = (K[]) new Object[len - 1];
        System.arraycopy(array, 1, a, 0, len - 1);
        array = a;
        return true;
    }


    public synchronized boolean removeLast(K obj) {
        if (peekLast() != obj) {
            return false;
        }
        array = Arrays.copyOf(array, array.length - 1);
        return true;
    }


    public Iterator<K> iterator() {
        return new Iterator<K>() {

            K[] a = array;
            int index;

            @Override
            public boolean hasNext() {
                return index < a.length;
            }

            @Override
            public K next() {
                return a[index++];
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }
}
