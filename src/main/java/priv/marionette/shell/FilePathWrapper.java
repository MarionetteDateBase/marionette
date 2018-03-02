package priv.marionette.shell;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * 特殊文件系统的本地封装
 * e.g. the distributed file system,the split file system,the socket file system
 *
 * @author Yue Yu
 * @create 2018-01-22 下午7:05
 **/
public abstract class FilePathWrapper extends  FilePath{

    private FilePath base; // 本体


    @Override
    public FilePath getPath(String path) {
        return create(path, unwrap(path));
    }

    private FilePathWrapper create(String path, FilePath base) {
        try {
            FilePathWrapper p = getClass().newInstance();
            p.name = path;
            p.base = base;
            return p;
        } catch (Exception e) {
            throw new IllegalArgumentException("Path: " + path, e);
        }
    }


    @Override
    public long size() {
        return base.size();
    }

    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        base.moveTo(((FilePathWrapper) newName).base, atomicReplace);
    }

    @Override
    public boolean createFile() {
        return base.createFile();
    }

    @Override
    public boolean exists() {
        return base.exists();
    }

    @Override
    public void delete() {
        base.delete();
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        List<FilePath> list = base.newDirectoryStream();
        for (int i = 0, len = list.size(); i < len; i++) {
            list.set(i, wrap(list.get(i)));
        }
        return list;
    }

    @Override
    public FilePath toRealPath() {
        return wrap(base.toRealPath());
    }

    @Override
    public FilePath getParent() {
        return wrap(base.getParent());
    }

    @Override
    public boolean isDirectory() {
        return base.isDirectory();
    }

    @Override
    public boolean isAbsolute() {
        return base.isAbsolute();
    }

    @Override
    public long lastModified() {
        return base.lastModified();
    }

    @Override
    public boolean canWrite() {
        return base.canWrite();
    }

    @Override
    public void createDirectory() {
        base.createDirectory();
    }


    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        return base.newOutputStream(append);
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return base.open(mode);
    }

    @Override
    public InputStream newInputStream() throws IOException {
        return base.newInputStream();
    }

    @Override
    public boolean setReadOnly() {
        return base.setReadOnly();
    }

    @Override
    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir) throws IOException {
        return wrap(base.createTempFile(suffix, deleteOnExit, inTempDir));
    }

    @Override
    public FilePath unwrap() {
        return unwrap(name);
    }

    /**
     * 去除文件路径中的scheme
     *
     * @param path the path including the scheme prefix
     * @return the base file path
     */
    protected FilePath unwrap(String path) {
        return FilePath.get(path.substring(getScheme().length() + 1));
    }

    /**
     * 封装filepath
     *
     * @param base the base path
     * @return the wrapped path
     */
    public FilePathWrapper wrap(FilePath base) {
        return base == null ? null : create(getPrefix() + base.name, base);
    }

    protected String getPrefix() {
        return getScheme() + ":";
    }

    protected FilePath getBase() {
        return base;
    }


}
