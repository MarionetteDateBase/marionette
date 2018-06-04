package priv.marionette.engine;

/**
 * 全系统用常量集
 *
 * @author Yue Yu
 * @create 2018-05-14 下午6:11
 **/
public class Constants {

    /**
     * 最少一个field的object所需的内存
     */
    // Java 6, 64 bit: 24
    // Java 6, 32 bit: 12
    public static final int MEMORY_OBJECT = 24;

    /**
     * 一个array所需的内存
     */
    public static final int MEMORY_ARRAY = 24;


    /**
     * 一个指针所需的内存
     */
    // Java 6, 64 bit: 8
    // Java 6, 32 bit: 4
    public static final int MEMORY_POINTER = 8;



    private Constants() {
        // utility class
    }



}
