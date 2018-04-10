package priv.marionette.ghost.btree;

/**
 * 迭代器中page的引用信息
 *
 * @author Yue Yu
 * @create 2018-04-10 下午12:51
 **/
public class CursorPos {
    /**
     * 当前的page
     */
    public Page page;

    /**
     * 当前的索引
     */
    public int index;

    /**
     * 父节点的page
     */
    public final CursorPos parent;

    public CursorPos(Page page, int index, CursorPos parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }
}