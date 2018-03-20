package priv.marionette.ghost;

import priv.marionette.cache.FilePathCache;
import priv.marionette.ghost.btree.BTreeWithMVCC;
import priv.marionette.shell.FIlePathNio;
import priv.marionette.shell.FilePath;
import priv.marionette.shell.FilePathDisk;
import priv.marionette.shell.FilePathEncrypt;
import priv.marionette.tools.DataUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
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

    protected final FreeSpaceBitSet freeSpace =
            new FreeSpaceBitSet(2, BTreeWithMVCC.BLOCK_SIZE);


    /**
     * 文件名
     */
    protected String fileName;

    /**
     * 文件是否为只读
     */
    protected boolean readOnly;

    /**
     * 文件长度
     */
    protected long fileSize;

    protected FileChannel file;

    /**
     * 压缩文件
     */
    protected FileChannel encryptedFile;

    /**
     * 文件锁
     */
    protected FileLock fileLock;

    @Override
    public String toString() {
        return fileName;
    }

    /**
     * 从指定offset读取len长的字节流
     * @param pos
     * @param len
     * @return
     */
    public ByteBuffer readFully(long pos, int len) {
        ByteBuffer dst = ByteBuffer.allocate(len);
        DataUtils.readFully(file, pos, dst);
        readCount.incrementAndGet();
        readBytes.addAndGet(len);
        return dst;
    }

    /**
     * 从指定offset写入完整的字节流
     * @param pos
     * @param src
     */
    public void writeFully(long pos, ByteBuffer src) {
        int len = src.remaining();
        fileSize = Math.max(fileSize, pos + len);
        DataUtils.writeFully(file, pos, src);
        writeCount.incrementAndGet();
        writeBytes.addAndGet(len);
    }


    /**
     * 打开文件
     * @param fileName
     * @param readOnly
     * @param encryptionKey
     */
    public void open(String fileName, boolean readOnly, char[] encryptionKey) {
        if (file != null) {
            return;
        }
        if (fileName != null) {
            FilePathCache.INSTANCE.getScheme();
            FilePath p = FilePath.get(fileName);
            if (p instanceof FilePathDisk &&
                    !fileName.startsWith(p.getScheme() + ":")) {
                FIlePathNio.class.getName();
                fileName = "nio:" + fileName;
            }
        }
        this.fileName = fileName;
        FilePath f = FilePath.get(fileName);
        FilePath parent = f.getParent();
        if (parent != null && !parent.exists()) {
            throw DataUtils.newIllegalArgumentException(
                    "Directory does not exist: {0}", parent);
        }
        if (f.exists() && !f.canWrite()) {
            readOnly = true;
        }
        this.readOnly = readOnly;
        try {
            file = f.open(readOnly ? "r" : "rw");
            if (encryptionKey != null) {
                byte[] key = FilePathEncrypt.getPasswordBytes(encryptionKey);
                encryptedFile = file;
                file = new FilePathEncrypt.FileEncrypt(fileName, key, file);
            }
            try {
                if (readOnly) {
                    fileLock = file.tryLock(0, Long.MAX_VALUE, true);
                } else {
                    fileLock = file.tryLock();
                }
            } catch (OverlappingFileLockException e) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_LOCKED,
                        "The file is locked: {0}", fileName, e);
            }
            if (fileLock == null) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_LOCKED,
                        "The file is locked: {0}", fileName);
            }
            fileSize = file.size();
        } catch (IOException e) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_READING_FAILED,
                    "Could not open file {0}", fileName, e);
        }
    }






}
