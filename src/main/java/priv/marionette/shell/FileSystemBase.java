package priv.marionette.shell;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
/**
 * 文件系统抽象基础类
 *
 * @author Yue Yu
 * @create 2018-01-04 下午5:45
 **/
public abstract class FileSystemBase extends FileChannel {

    @Override
    public abstract long size() throws IOException;

    @Override
    public abstract long position() throws IOException;

    @Override
    public abstract FileChannel position(long newPosition) throws IOException;

    @Override
    public abstract int read(ByteBuffer dst) throws IOException;

    @Override
    public abstract int write(ByteBuffer src) throws IOException;

    @Override
    public abstract FileChannel truncate(long size) throws IOException;

    @Override
    public void force(boolean metaData) throws IOException {
        // ignore
    }

    @Override
    protected void implCloseChannel() throws IOException {
        // ignore
    }

    @Override
    public FileLock lock(long position, long size, boolean shared)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size)
            throws IOException {
        throw new UnsupportedOperationException();
    }


    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int read(ByteBuffer dst, long position)
            throws IOException {
        long oldPos = position();
        position(position);
        int len = read(dst);
        position(oldPos);
        return len;
    }

    @Override
    public synchronized int write(ByteBuffer src, long position)
            throws IOException {
        long oldPos = position();
        position(position);
        int len = write(src);
        position(oldPos);
        return len;
    }

}
