package priv.marionette.ghost.btree;

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
public class Cursor {



}
