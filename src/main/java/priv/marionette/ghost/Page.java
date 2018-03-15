package priv.marionette.ghost;

/**
 * B树的节点
 * 文件数据结构按照算法导论(第三版)给出如下定义:
 * 每个B树的节点拥有一个左子节点，这个节点的key大于子节点key list的最大值
 *
 * 结构化文件(offset从小到大)：
 * page length: int
 * checksum : short
 * map id : varInt
 * number of keys :varInt
 * type : (0: 叶子节点, 1: 内部或根节点; +2: compressed)
 * compressed: bytes saved (varInt) keys{
 * leaf: values (one for each key)
 * node: children (1 more than keys)
 * }
 *
 * @author Yue Yu
 * @create 2018-03-13 下午4:35
 **/
public class Page {

}
