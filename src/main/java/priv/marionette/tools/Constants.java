package priv.marionette.tools;

import java.nio.charset.Charset;
import java.sql.ResultSet;

/**
 * 系统内常量
 *
 * @author Yue Yu
 * @create 2018-01-09 下午3:24
 **/
public class Constants {

    /**
     * The build id is incremented for each public release.
     */
    public static final int BUILD_ID = 196;


    /**
     * The major version of this database.
     */
    public static final int VERSION_MAJOR = 1;

    /**
     * The minor version of this database.
     */
    public static final int VERSION_MINOR = 4;


    /**
     * The block size for I/O operations.
     */
    public static final int IO_BUFFER_SIZE = 4 * 1024;

    /**
     * Name of the character encoding format.
     */
    public static final Charset UTF8 = Charset.forName("UTF-8");


    private Constants() {
        // utility class
    }


}
