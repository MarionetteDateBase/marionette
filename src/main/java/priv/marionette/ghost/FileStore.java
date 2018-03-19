package priv.marionette.ghost;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据的持久化
 *
 * @author Yue Yu
 * @create 2018-01-24 下午12:53
 **/
public class FileStore {

    /**
     * read次数
     */
    protected final AtomicLong readCount = new AtomicLong(0);

    /**
     * 读取的字节流长度
     */
    protected final AtomicLong readBytes = new AtomicLong(0);

    /**
     * write次数
     */
    protected final AtomicLong writeCount = new AtomicLong(0);

    /**
     * write的字节流的长度
     */
    protected final AtomicLong writeBytes = new AtomicLong(0);

}
