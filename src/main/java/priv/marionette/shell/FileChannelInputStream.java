package priv.marionette.shell;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 以InputStream封装FileChannel
 *
 * @author Yue Yu
 * @create 2018-01-24 下午3:39
 **/
public class FileChannelInputStream extends InputStream{
    private final FileChannel channel;
    private final boolean closeChannel;

    private ByteBuffer buffer;
    private long pos;

    public FileChannelInputStream(FileChannel channel, boolean closeChannel) {
        this.channel = channel;
        this.closeChannel = closeChannel;
    }

    @Override
    public int read() throws IOException {
        if (buffer == null) {
            buffer = ByteBuffer.allocate(1);
        }
        buffer.rewind();
        int len = channel.read(buffer, pos++);
        if (len < 0) {
            return -1;
        }
        return buffer.get(0) & 0xff;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ByteBuffer buff = ByteBuffer.wrap(b, off, len);
        int read = channel.read(buff, pos);
        if (read == -1) {
            return -1;
        }
        pos += read;
        return read;
    }

    @Override
    public void close() throws IOException {
        if (closeChannel) {
            channel.close();
        }
    }

}
