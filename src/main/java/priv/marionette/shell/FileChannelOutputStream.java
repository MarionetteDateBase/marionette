package priv.marionette.shell;

import priv.marionette.tools.FileUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 以OutputStream封装FileChannel
 *
 * @author Yue Yu
 * @create 2018-01-24 下午3:39
 **/
public class FileChannelOutputStream extends OutputStream{
    private final FileChannel channel;
    private final byte[] buffer = { 0 };

    /**
     * 从FileChannel创建OutputStream对象
     *
     * @param channel the file channel
     * @param append true for append mode, false for truncate and overwrite
     */
    public FileChannelOutputStream(FileChannel channel, boolean append)
            throws IOException {
        this.channel = channel;
        if (append) {
            channel.position(channel.size());
        } else {
            channel.position(0);
            channel.truncate(0);
        }
    }

    @Override
    public void write(int b) throws IOException {
        buffer[0] = (byte) b;
        FileUtils.writeFully(channel, ByteBuffer.wrap(buffer));
    }

    @Override
    public void write(byte[] b) throws IOException {
        FileUtils.writeFully(channel, ByteBuffer.wrap(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        FileUtils.writeFully(channel, ByteBuffer.wrap(b, off, len));
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
