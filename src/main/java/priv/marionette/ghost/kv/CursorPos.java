package priv.marionette.ghost.kv;

/**
 * BP树迭代器
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
    public CursorPos parent;

    public CursorPos(Page page, int index, CursorPos parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }
}
