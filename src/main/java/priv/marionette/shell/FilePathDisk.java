package priv.marionette.shell;

import priv.marionette.Exception.DbException;
import priv.marionette.api.ErrorCode;
import priv.marionette.tools.FileUtils;
import priv.marionette.tools.IOUtils;
import priv.marionette.tools.New;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.util.ArrayList;
import java.util.List;

/**
 * 持久化主存数据到辅存
 *
 * @author Yue Yu
 * @create 2018-01-08 下午7:43
 **/
public class FilePathDisk extends FilePath{

    private static final String CLASSPATH_PREFIX = "classpath:";

    @Override
    public long size() {
        return new File(name).length();
    }



    /**
     * 扩展用户文件目录下文件的绝对路径
     *
     * @param fileName the file name
     * @return the native file name
     */
    public static String expandUserHomeDirectory(String fileName) {
        if (fileName.startsWith("~") && (fileName.length() == 1 ||
                fileName.startsWith("~/"))) {
            String userDir = SystemProperties.USER_HOME;
            fileName = userDir + fileName.substring(1);
        }
        return fileName;
    }

    /**
     * 文件名的本地适配转换
     *
     * @param fileName the file name
     * @return the native file name
     */
    protected static String translateFileName(String fileName) {
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("file:")) {
            fileName = fileName.substring("file:".length());
        }
        return expandUserHomeDirectory(fileName);
    }


    @Override
    public FilePathDisk getPath(String path) {
        FilePathDisk p = new FilePathDisk();
        p.name = translateFileName(path);
        return p;
    }


    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        File oldFile = new File(name);
        File newFile = new File(newName.name);
        if (oldFile.getAbsolutePath().equals(newFile.getAbsolutePath())) {
            return;
        }
        if (!oldFile.exists()) {
            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
                    name + " (not found)",
                    newName.name);
        }
        if (atomicReplace) {
            boolean ok = oldFile.renameTo(newFile);
            if (ok) {
                return;
            }
            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
                    new String[]{name, newName.name});
        }
        if (newFile.exists()) {
            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
                    new String[] { name, newName + " (exists)" });
        }
        for (int i = 0; i < SystemProperties.MAX_FILE_RETRY; i++) {
            IOUtils.trace("rename", name + " >" + newName, null);
            boolean ok = oldFile.renameTo(newFile);
            if (ok) {
                return;
            }
            wait(i);
        }
        throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
                new String[]{name, newName.name});
    }

    private static void wait(int i) {
        if (i == 8) {
            System.gc();
        }
        try {
            // sleep at most 256 ms
            long sleep = Math.min(256, i * i);
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    public boolean createFile() {
        File file = new File(name);
        for (int i = 0; i < SystemProperties.MAX_FILE_RETRY; i++) {
            try {
                return file.createNewFile();
            } catch (IOException e) {
                // 'access denied' is really a concurrent access problem
                wait(i);
            }
        }
        return false;
    }

    @Override
    public boolean exists() {
        return new File(name).exists();
    }


    @Override
    public void delete() {
        File file = new File(name);
        for (int i = 0; i < SystemProperties.MAX_FILE_RETRY; i++) {
            IOUtils.trace("delete", name, null);
            boolean ok = file.delete();
            if (ok || !file.exists()) {
                return;
            }
            wait(i);
        }
        throw DbException.get(ErrorCode.FILE_DELETE_FAILED_1, name);
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        ArrayList<FilePath> list = New.arrayList();
        File f = new File(name);
        try {
            String[] files = f.list();
            if (files != null) {
                String base = f.getCanonicalPath();
                if (!base.endsWith(SystemProperties.FILE_SEPARATOR)) {
                    base += SystemProperties.FILE_SEPARATOR;
                }
                for (int i = 0, len = files.length; i < len; i++) {
                    list.add(getPath(base + files[i]));
                }
            }
            return list;
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    private static boolean canWriteInternal(File file) {
        try {
            if (!file.canWrite()) {
                return false;
            }
        } catch (Exception e) {
            // workaround for GAE which throws a
            // java.security.AccessControlException
            return false;
        }
        //修复jdk的bug
        // File.canWrite()在windows平台下无法获取真实的权限状态，所以使用mode"rw"去实际打开一下文件来测试权限
        // Bug详情请见http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4420020
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(file, "rw");
            return true;
        } catch (FileNotFoundException e) {
            return false;
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }


    @Override
    public boolean canWrite() {
        return canWriteInternal(new File(name));
    }


    @Override
    public boolean setReadOnly() {
        File f = new File(name);
        return f.setReadOnly();
    }


    @Override
    public FilePathDisk toRealPath() {
        try {
            String fileName = new File(name).getCanonicalPath();
            return getPath(fileName);
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    @Override
    public FilePath getParent() {
        String p = new File(name).getParent();
        return p == null ? null : getPath(p);
    }

    @Override
    public boolean isDirectory() {
        return new File(name).isDirectory();
    }

    @Override
    public boolean isAbsolute() {
        return new File(name).isAbsolute();
    }

    @Override
    public long lastModified() {
        return new File(name).lastModified();
    }

    @Override
    public void createDirectory() {
        File dir = new File(name);
        for (int i = 0; i < SystemProperties.MAX_FILE_RETRY; i++) {
            if (dir.exists()) {
                if (dir.isDirectory()) {
                    return;
                }
                throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
                        name + " (a file with this name already exists)");
            } else if (dir.mkdir()) {
                return;
            }
            wait(i);
        }
        throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1, name);
    }


    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        try {
            File file = new File(name);
            File parent = file.getParentFile();
            if (parent != null) {
                FileUtils.createDirectories(parent.getAbsolutePath());
            }
            FileOutputStream out = new FileOutputStream(name, append);
            IOUtils.trace("openFileOutputStream", name, out);
            return out;
        } catch (IOException e) {
            freeMemoryAndFinalize();
            return new FileOutputStream(name);
        }
    }

    /**
     * 发生IO异常之后回收所有句柄资源并且invoke finalization
     */
    static void freeMemoryAndFinalize() {
        IOUtils.trace("freeMemoryAndFinalize", null, null);
        Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        for (int i = 0; i < 16; i++) {
            rt.gc();
            long now = rt.freeMemory();
            rt.runFinalization();
            if (now == mem) {
                break;
            }
            mem = now;
        }
    }

    @Override
    public InputStream newInputStream() throws IOException {
        if (name.matches("[a-zA-Z]{2,19}:.*")) {
            // if the ':' is in position 1, a windows file access is assumed:
            // C:.. or D:, and if the ':' is not at the beginning, assume its a
            // file name with a colon
            if (name.startsWith(CLASSPATH_PREFIX)) {
                String fileName = name.substring(CLASSPATH_PREFIX.length());
                // Force absolute resolution in Class.getResourceAsStream
                if (!fileName.startsWith("/")) {
                    fileName = "/" + fileName;
                }
                InputStream in = getClass().getResourceAsStream(fileName);
                if (in == null) {
                    // ClassLoader.getResourceAsStream doesn't need leading "/"
                    in = Thread.currentThread().getContextClassLoader().
                            getResourceAsStream(fileName.substring(1));
                }
                if (in == null) {
                    throw new FileNotFoundException("resource " + fileName);
                }
                return in;
            }
            // otherwise an URL is assumed
            URL url = new URL(name);
            InputStream in = url.openStream();
            return in;
        }
        FileInputStream in = new FileInputStream(name);
        IOUtils.trace("openFileInputStream", name, in);
        return in;
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        FileDisk f;
        try {
            f = new FileDisk(name, mode);
            IOUtils.trace("open", name, f);
        } catch (IOException e) {
            freeMemoryAndFinalize();
            try {
                f = new FileDisk(name, mode);
            } catch (IOException e2) {
                throw e;
            }
        }
        return f;
    }

    @Override
    public String getScheme() {
        return "file";
    }


    @Override
    public FilePath createTempFile(String suffix, boolean deleteOnExit,
                                   boolean inTempDir) throws IOException {
        String fileName = name + ".";
        String prefix = new File(fileName).getName();
        File dir;
        if (inTempDir) {
            dir = new File(System.getProperty("java.io.tmpdir", "."));
        } else {
            dir = new File(fileName).getAbsoluteFile().getParentFile();
        }
        FileUtils.createDirectories(dir.getAbsolutePath());
        while (true) {
            File f = new File(dir, prefix + getNextTempFileNamePart(false) + suffix);
            if (f.exists() || !f.createNewFile()) {
                // in theory, the random number could collide
                getNextTempFileNamePart(true);
                continue;
            }
            if (deleteOnExit) {
                try {
                    f.deleteOnExit();
                } catch (Throwable e) {
                    // sometimes this throws a NullPointerException
                    // at java.io.DeleteOnExitHook.add(DeleteOnExitHook.java:33)
                    // we can ignore it
                }
            }
            return get(f.getCanonicalPath());
        }
    }

}


/**
 *磁盘文件随机读写接口,使用RandomAccessFile的方式
 */
class  FileDisk extends  FileSystemBase{
    private final RandomAccessFile file;
    private final String name;
    private final boolean readOnly;

    FileDisk(String fileName, String mode) throws FileNotFoundException {
        this.file = new RandomAccessFile(fileName, mode);
        this.name = fileName;
        this.readOnly = mode.equals("r");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        String m = SystemProperties.SYNC_METHOD;
        if ("".equals(m)) {
            // do nothing
        } else if ("sync".equals(m)) {
            file.getFD().sync();
        } else if ("force".equals(m)) {
            file.getChannel().force(true);
        } else if ("forceFalse".equals(m)) {
            file.getChannel().force(false);
        } else {
            file.getFD().sync();
        }
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        // compatibility with JDK FileChannel#truncate
        if (readOnly) {
            throw new NonWritableChannelException();
        }
        if (newLength < file.length()) {
            file.setLength(newLength);
        }
        return this;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
                                         boolean shared) throws IOException {
        return file.getChannel().tryLock(position, size, shared);
    }

    @Override
    public void implCloseChannel() throws IOException {
        file.close();
    }

    @Override
    public long position() throws IOException {
        return file.getFilePointer();
    }

    @Override
    public long size() throws IOException {
        return file.length();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int len = file.read(dst.array(), dst.arrayOffset() + dst.position(),
                dst.remaining());
        if (len > 0) {
            dst.position(dst.position() + len);
        }
        return len;
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        file.seek(pos);
        return this;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        file.write(src.array(), src.arrayOffset() + src.position(), len);
        src.position(src.position() + len);
        return len;
    }

    @Override
    public String toString() {
        return name;
    }





}
