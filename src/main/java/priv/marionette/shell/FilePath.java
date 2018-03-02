package priv.marionette.shell;


import priv.marionette.tools.MathUtils;
import priv.marionette.tools.New;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 文件访问基础类
 *
 * 提供对文件访问的路径，整合Java7中的java.nio.file.Path和java.nio.file.FileSystem，以更简单的方式操作和便于对老版本Java的使用支持
 *
 * @author Yue Yu
 * @create 2018-01-04 下午6:21
 **/
public abstract class FilePath {

    private static FilePath defaultProvider;

    private static Map<String, FilePath> providers;

    /**
     * 生成临时文件所用的前缀
     */
    private static String tempRandom;
    private static long tempSequence;

    /**
     * 文件的完整访问路径
     */
    protected String name;

    /**
    * 得到默认文件访问路径实现的前缀，对应java.nio.file.spi.FileSystemProvider.getScheme
    */
    public abstract String getScheme();


    /**
     * 根据Scheme将文本路径转换为FilePath对象，对应java.nio.file.spi.FileSystemProvider.getPath
     */
    public abstract FilePath getPath(String path);

    private static void registerDefaultProviders() {
        if (providers == null || defaultProvider == null) {
            Map<String, FilePath> map = new ConcurrentHashMap<>(
                    New.<String, FilePath>hashMap());
            for (String c : new String[] {
                    "priv.marionette.shell.FilePathDisk",
                    "priv.marionette.shell.FilePathMem",
                    "priv.marionette.shell.FilePathMemLZF",
                    "priv.marionette.shell.FilePathNioMem",
                    "priv.marionette.shell.FilePathNioMemLZF",
                    "priv.marionette.shell.FilePathSplit",
                    "priv.marionette.shell.FilePathNio",
                    "priv.marionette.shell.FilePathNioMapped",
                    "priv.marionette.shell.FilePathZip",
                    "priv.marionette.shell.FilePathRetryOnInterrupt"
            }) {
                try {
                    FilePath p = (FilePath) Class.forName(c).newInstance();
                    map.put(p.getScheme(), p);
                    if (defaultProvider == null) {
                        defaultProvider = p;
                    }
                } catch (Exception e) {
                    // ignore - the files may be excluded in purpose
                }
            }
            providers = map;
        }
    }

    /**
     * 通过文本路径获取FilePath对象
     *
     * @param path the path
     * @return the file path object
     */
    public static FilePath get(String path) {
        path = path.replace('\\', '/');
        int index = path.indexOf(':');
        registerDefaultProviders();
        if (index < 2) {
            // use the default provider if no prefix or
            // only a single character (drive name)
            return defaultProvider.getPath(path);
        }
        String scheme = path.substring(0, index);
        FilePath p = providers.get(scheme);
        if (p == null) {
            // provider not found - use the default
            p = defaultProvider;
        }
        return p.getPath(path);
    }

    /**
     * 注册一个新的自定义FilePath实现
     *
     * @param provider the file provider
     */
    public static void register(FilePath provider) {
        registerDefaultProviders();
        providers.put(provider.getScheme(), provider);
    }

    /**
     * 移除一个FilePath实现
     *
     * @param provider the file provider
     */
    public static void unregister(FilePath provider) {
        registerDefaultProviders();
        providers.remove(provider.getScheme());
    }


    /**
     * 获取文件的字节长度
     *
     * @return the size in bytes
     */
    public abstract long size();



    /**
     * 重命名文件
     *
     * @param newName the new fully qualified file name
     * @param atomicReplace whether the move should be atomic, and the target
     *            file should be replaced if it exists and replacing is possible
     */
    public abstract void moveTo(FilePath newName, boolean atomicReplace);


    /**
     * 创建文件
     *
     * @return true if creating was successful
     */
    public abstract boolean createFile();

    /**
     * 检查文件是否存在
     *
     * @return true if it exists
     */
    public abstract boolean exists();

    /**
     * 如果文件或者目录存在则删除，如果目录下没有文件那么只删除目录
     */
    public abstract void delete();

    /**
     * 将目录下的文件返回
     *
     * @return the list of fully qualified file names
     */
    public abstract List<FilePath> newDirectoryStream();

    /**
     * 返回文件绝对路径
     *
     * @return the normalized file name
     */
    public abstract FilePath toRealPath();

    /**
     * 文件目录双亲迭代
     *
     * @return the parent directory name
     */
    public abstract FilePath getParent();

    /**
     * 检测是否是一个文件目录
     *
     * @return true if it is a directory
     */
    public abstract boolean isDirectory();

    /**
     * 检测此抽象文件路径是否是绝对路径
     *
     * @return if the file name is absolute
     */
    public abstract boolean isAbsolute();

    /**
     * 获取一个文件的最近修改日期
     *
     * @return the last modified date
     */
    public abstract long lastModified();

    /**
     * 判断文件是否可写
     *
     * @return if the file is writable
     */
    public abstract boolean canWrite();

    /**
     * 创建一个文件目录
     */
    public abstract void createDirectory();

    /**
     * 获得文件或文件目录的name
     *
     * @return the last element of the path
     */
    public String getName() {
        int idx = Math.max(name.indexOf(':'), name.lastIndexOf('/'));
        return idx < 0 ? name : name.substring(idx + 1);
    }

    /**
     * 创建一个文件写入流
     *
     * @param append if true, the file will grow, if false, the file will be
     *            truncated first
     * @return the output stream
     */
    public abstract OutputStream newOutputStream(boolean append) throws IOException;

    /**
     * 创建一个FileChannel用来随机访问文件
     *
     * @param mode the access mode. Supported are r, rw, rws, rwd
     * @return the file object
     */
    public abstract FileChannel open(String mode) throws IOException;

    /**
     * 创建一个文件读入流
     *
     * @return the input stream
     */
    public abstract InputStream newInputStream() throws IOException;

    /**
     * 设置文件为只读
     *
     * @return true if the call was successful
     */
    public abstract boolean setReadOnly();

    /**
     * 创建一个临时文件
     *
     * @param suffix the suffix
     * @param deleteOnExit if the file should be deleted when the virtual
     *            machine exists
     * @param inTempDir if the file should be stored in the temporary directory
     * @return the name of the created file
     */
    @SuppressWarnings("unused")
    public FilePath createTempFile(String suffix, boolean deleteOnExit,
                                   boolean inTempDir) throws IOException {
        while (true) {
            FilePath p = getPath(name + getNextTempFileNamePart(false) + suffix);
            if (p.exists() || !p.createFile()) {
                // in theory, the random number could collide
                getNextTempFileNamePart(true);
                continue;
            }
            p.open("rw").close();
            return p;
        }
    }

    /**
     * 不断追寻成功生成文件的可能性～
     *
     * @param newRandom if the random part of the filename should change
     * @return the file name part
     */
    protected static synchronized String getNextTempFileNamePart(
            boolean newRandom) {
        if (newRandom || tempRandom == null) {
            tempRandom = MathUtils.randomInt(Integer.MAX_VALUE) + ".";
        }
        return tempRandom + tempSequence++;
    }

    /**
     * 返回文件名
     *
     * @return the path as a string
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * 去除文件系统前缀
     *
     * @return the unwrapped path
     */
    public FilePath unwrap() {
        return this;
    }







}
