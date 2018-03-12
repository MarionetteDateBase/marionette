package priv.marionette.ghost;

import priv.marionette.tools.DataUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

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
     * 数据块长度的总和
     */
    public int len;


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
    public long next;

    Chunk(int id) {
        this.id = id;
    }


    /**
     * 读取Chunk Header
     *
     **/
    public static Chunk readChunkHeader(ByteBuffer buff, long start) {
        int pos = buff.position();
        byte[] data = new byte[Math.min(buff.remaining(), MAX_HEADER_LENGTH)];
        buff.get(data);
        try {
            for (int i = 0; i < data.length; i++) {
                if (data[i] == '\n') {
                    // set the position to the start of the first page
                    buff.position(pos + i + 1);
                    String s = new String(data, 0, i, StandardCharsets.ISO_8859_1).trim();
                    return fromString(s);
                }
            }
        } catch (Exception e) {
            // 意外异常
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupt reading chunk at position {0}", start, e);
        }
        throw DataUtils.newIllegalStateException(
                DataUtils.ERROR_FILE_CORRUPT,
                "File corrupt reading chunk at position {0}", start);
    }


    /**
     * 从一个字符串构造Chunk
     *
     **/
    public static Chunk fromString(String s) {
        HashMap<String, String> map = DataUtils.parseMap(s);
        int id = DataUtils.readHexInt(map, "chunk", 0);
        Chunk c = new Chunk(id);
        c.block = DataUtils.readHexLong(map, "block", 0);
        c.len = DataUtils.readHexInt(map, "len", 0);
        c.pageCount = DataUtils.readHexInt(map, "pages", 0);
        c.pageCountLive = DataUtils.readHexInt(map, "livePages", c.pageCount);
        c.mapId = DataUtils.readHexInt(map, "map", 0);
        c.maxLen = DataUtils.readHexLong(map, "max", 0);
        c.maxLenLive = DataUtils.readHexLong(map, "liveMax", c.maxLen);
        c.metaRootPos = DataUtils.readHexLong(map, "root", 0);
        c.time = DataUtils.readHexLong(map, "time", 0);
        c.unused = DataUtils.readHexLong(map, "unused", 0);
        c.version = DataUtils.readHexLong(map, "version", id);
        c.next = DataUtils.readHexLong(map, "next", 0);
        return c;
    }








}
