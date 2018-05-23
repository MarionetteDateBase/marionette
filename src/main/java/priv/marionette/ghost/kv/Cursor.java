package priv.marionette.ghost.kv;

import priv.marionette.tools.DataUtils;

import java.util.Iterator;

/**
 * B树的先序遍历
 * 与传统先序遍历逻辑类似，在dfs到最左叶子节点后，
 * 开始遍历page内部keySet，由于B树的特殊性，
 * 用page节点信息结合函数递归来隐式压栈，
 * 一方面也是因为b树的树高比较小，如果是传统的
 * AVL、red-black树由于树高可能会导致函数控制栈溢出，
 * 故在先序遍历时必须显式压栈
 *
 *
 * @author Yue Yu
 * @create 2018-04-10 下午12:23
 **/
public class Cursor<K, V> implements Iterator<K> {

    private final K to;
    private CursorPos cursorPos;
    private CursorPos keeper;
    private K current;
    private K last;
    private V lastValue;
    private Page lastPage;

    public Cursor(Page root, K from) {
        this(root, from, null);
    }

    public Cursor(Page root, K from, K to) {
        this.cursorPos = traverseDown(root, from);
        this.to = to;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean hasNext() {
        if (cursorPos != null) {
            while (current == null) {
                Page page = cursorPos.page;
                int index = cursorPos.index;
                if (index >= (page.isLeaf() ? page.getKeyCount() : page.map.getChildPageCount(page))) {
                    CursorPos tmp = cursorPos;
                    cursorPos = cursorPos.parent;
                    tmp.parent = keeper;
                    keeper = tmp;
                    if(cursorPos == null)
                    {
                        return false;
                    }
                } else {
                    while (!page.isLeaf()) {
                        page = page.getChildPage(index);
                        if (keeper == null) {
                            cursorPos = new CursorPos(page, 0, cursorPos);
                        } else {
                            CursorPos tmp = keeper;
                            keeper = keeper.parent;
                            tmp.parent = cursorPos;
                            tmp.page = page;
                            tmp.index = 0;
                            cursorPos = tmp;
                        }
                        index = 0;
                    }
                    K key = (K) page.getKey(index);
                    if (to != null && page.map.getKeyType().compare(key, to) > 0) {
                        return false;
                    }
                    current = last = key;
                    lastValue = (V) page.getValue(index);
                    lastPage = page;
                }
                ++cursorPos.index;
            }
        }
        return current != null;
    }

    @Override
    public K next() {
        if(!hasNext()) {
            return null;
        }
        current = null;
        return last;
    }

    /**
     * 最后读到的key
     *
     * @return the key or null
     */
    public K getKey() {
        return last;
    }

    /**
     * 最后读到的value
     *
     * @return the value or null
     */
    public V getValue() {
        return lastValue;
    }

    /**
     * 最后被访问的page
     *
     * @return the page
     */
    Page getPage() {
        return lastPage;
    }

    public void skip(long n) {
        if (n < 10) {
            while (n-- > 0 && hasNext()) {
                next();
            }
        } else if(hasNext()) {
            assert cursorPos != null;
            CursorPos cp = cursorPos;
            CursorPos parent;
            while ((parent = cp.parent) != null) cp = parent;
            Page root = cp.page;
            @SuppressWarnings("unchecked")
            MVBTreeMap<K, ?> map = (MVBTreeMap<K, ?>) root.map;
            long index = map.getKeyIndex(next());
            last = map.getKey(index + n);
            this.cursorPos = traverseDown(root, last);
        }
    }

    @Override
    public void remove() {
        throw DataUtils.newUnsupportedOperationException(
                "Removal is not supported");
    }

    /**
     * 从指定节点开始将key值大于等于的最左子节点，其实就是在压栈
     *
     * @param p the page to start from
     * @param key the key to search, null means search for the first key
     */
    public static CursorPos traverseDown(Page p, Object key) {
        CursorPos cursorPos = null;
        while (!p.isLeaf()) {
            assert p.getKeyCount() > 0;
            int index = 0;
            if(key != null) {
                index = p.binarySearch(key) + 1;
                if (index < 0) {
                    index = -index;
                }
            }
            cursorPos = new CursorPos(p, index, cursorPos);
            p = p.getChildPage(index);
        }
        int index = 0;
        if(key != null) {
            index = p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
            }
        }
        return new CursorPos(p, index, cursorPos);
    }

}
