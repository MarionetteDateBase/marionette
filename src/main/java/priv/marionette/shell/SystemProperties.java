package priv.marionette.shell;

import priv.marionette.tools.MathUtils;
import priv.marionette.tools.Utils;

/**
 * 进程全局配置
 *
 * @author Yue Yu
 * @create 2018-01-09 下午3:11
 **/
public class SystemProperties {

    /**
     * System property <code>file.separator</code> (default: /).<br />
     * It is usually set by the system, and used to build absolute file names.
     */
    public static final String FILE_SEPARATOR =
            Utils.getProperty("file.separator", "/");


    /**
     * System property <code>user.home</code> (empty string if not set).<br />
     * It is usually set by the system, and used as a replacement for ~ in file
     * names.
     */
    public static final String USER_HOME =
            Utils.getProperty("user.home", "");


    /**
     * System property <code>h2.enableAnonymousTLS</code> (default: true).<br />
     * When using TLS connection, the anonymous cipher suites should be enabled.
     */
    public static final boolean ENABLE_ANONYMOUS_TLS =
            Utils.getProperty("h2.enableAnonymousTLS", true);

    /**
     * System property <code>h2.check</code> (default: true).<br />
     * Assertions in the database engine.
     */
    public static final boolean CHECK =
            Utils.getProperty("h2.check", true);


    /**
     * System property <code>h2.maxFileRetry</code> (default: 16).<br />
     * Number of times to retry file delete and rename. in Windows, files can't
     * be deleted if they are open. Waiting a bit can help (sometimes the
     * Windows Explorer opens the files for a short time) may help. Sometimes,
     * running garbage collection may close files if the user forgot to call
     * Connection.close() or InputStream.close().
     */
    public static final int MAX_FILE_RETRY =
            Math.max(1, Utils.getProperty("h2.maxFileRetry", 16));


    /**
     * System property <code>h2.objectCache</code> (default: true).<br />
     * Cache commonly used values (numbers, strings). There is a shared cache
     * for all values.
     */
    public static final boolean OBJECT_CACHE =
            Utils.getProperty("h2.objectCache", true);


    /**
     * System property <code>h2.objectCacheSize</code> (default: 1024).<br />
     * The maximum number of objects in the cache.
     * This value must be a power of 2.
     */
    public static final int OBJECT_CACHE_SIZE;
    static {
        try {
            OBJECT_CACHE_SIZE = MathUtils.nextPowerOf2(
                    Utils.getProperty("h2.objectCacheSize", 1024));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Invalid h2.objectCacheSize", e);
        }
    }


    /**
     * System property <code>h2.socketConnectTimeout</code>
     * (default: 2000).<br />
     * The timeout in milliseconds to connect to a server.
     */
    public static final int SOCKET_CONNECT_TIMEOUT =
            Utils.getProperty("h2.socketConnectTimeout", 2000);


    /**
     * System property <code>h2.syncMethod</code> (default: sync).<br />
     * What method to call when closing the database, on checkpoint, and on
     * CHECKPOINT SYNC. The following options are supported:
     * "sync" (default): RandomAccessFile.getFD().sync();
     * "force": RandomAccessFile.getChannel().force(true);
     * "forceFalse": RandomAccessFile.getChannel().force(false);
     * "": do not call a method (fast but there is a risk of data loss
     * on power failure).
     */
    public static final String SYNC_METHOD =
            Utils.getProperty("h2.syncMethod", "sync");

    /**
     * System property <code>h2.traceIO</code> (default: false).<br />
     * Trace all I/O operations.
     */
    public static final boolean TRACE_IO =
            Utils.getProperty("h2.traceIO", false);



    private SystemProperties() {
        // utility class
    }




}
