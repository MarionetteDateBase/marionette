package priv.marionette.ghost;

/**
 * 一个Chunk对应一颗B-Tree，由至少一个page(树的节点)组成，
 * 每个page大小为一次IO的大小(默认4096Bytes，对应8个磁盘扇区)，
 * 最多可以同时存在2^26次方个Chunk，每个Chunk最大2GB
 *
 * @author Yue Yu
 * @create 2018-01-04 下午6:48
 **/
public class Chunk {


    /**
     * 最大ID
     *
     **/
    public static  final  int MAX_ID = (1 << 26) -1;


    /**
     * header长度
     *
     **/
    public static  final  int MAX_HEADER_LENGTH = 1024;


    /**
     * Chunk footer的长度
     *
     **/
    public static  final  int FOOTER_LENGTH = 128;

    /**
     * 当前Chunk的id
     *
     */
    public final int id;


    /**
     * 起始数据块的位置
     */
    public long block;


    /**
     * 当前Chunk中Page的数量
     *
     */
    public int pageCount;


    /**
     * 当前Chunk中有被引用的Page数量
     *
     */
    public int pageCountLive;

    /**
     * 所有page的最大长度和
     *
     */
    public long maxLen;

    /**
     * 被引用状态下的Page的最大长度和
     *
     **/
    public long  maxLenLive;


    /**
     * 垃圾回收优先级，0为最高
     *
     **/
    public int collectPriority;


    /**
     * meta信息的起始offset
     *
     **/
    public long metaRootPos;


    /**
     * version信息的起始offset
     *
     **/
    public long version;

    /**
     * Chunk的创建时间(unix时间戳)
     *
     **/
    public long time;

    /**
     * Chunk在当前版本被停用的时间(unix时间戳)
     *
     **/
    public long unused;

    /**
     * 最后使用的mapId
     *
     **/
    public int mapId;


    /**
     * 下一个Chunk的offset
     *
     **/
    public int next;

    Chunk(int id){
        this.id = id;
    }





}
