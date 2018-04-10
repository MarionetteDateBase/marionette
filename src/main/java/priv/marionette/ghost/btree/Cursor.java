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
     * 从指定的key开始搜索下一个lager值
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



}
