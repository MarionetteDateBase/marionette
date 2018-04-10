package priv.marionette.ghost.btree;

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

    private final MVMap<K, ?> map;
    private final K from;
    private CursorPos pos;
    private K current, last;
    private V currentValue, lastValue;
    private Page lastPage;
    private final Page root;
    private boolean initialized;

    Cursor(MVMap<K, ?> map, Page root, K from) {
        this.map = map;
        this.root = root;
        this.from = from;
    }



    /**
     * 从指定的p节点开始压栈
     * @param p
     * @param from
     */
    private void min(Page p, K from) {
        while (true) {
            if (p.isLeaf()) {
                int x = from == null ? 0 : p.binarySearch(from);
                if (x < 0) {
                    x = -x - 1;
                }
                pos = new CursorPos(p, x, pos);
                break;
            }
            int x = from == null ? -1 : p.binarySearch(from);
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            pos = new CursorPos(p, x + 1, pos);
            p = p.getChildPage(x);
        }
    }


    /**
     * 从栈低pop出非递减序列中的一个值，耗尽当前节点后向上backtrace,
     * 然后从新一个pop出的栈节点的开始重新压栈，故此间2个函数结合，
     * 实现了对b树的深度优先搜索
     */
    @SuppressWarnings("unchecked")
    private void fetchNext() {
        while (pos != null) {
            if (pos.index < pos.page.getKeyCount()) {
                int index = pos.index++;
                current = (K) pos.page.getKey(index);
                currentValue = (V) pos.page.getValue(index);
                return;
            }
            pos = pos.parent;
            if (pos == null) {
                break;
            }
            if (pos.index < map.getChildPageCount(pos.page)) {
                min(pos.page.getChildPage(pos.index++), null);
            }
        }
        current = null;
    }



}
