package priv.marionette.shell;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.*;

/**
 * NIO方式将数据持久化到辅存
 *
 * @author Yue Yu
 * @create 2018-01-23 下午12:29
 **/
public class FIlePathNio extends FilePathWrapper {
    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileNio(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return "nio";
    }
}


/**
 * 使用FileChannel读写文件
 *
 */
class FileNio extends  FileSystemBase{
    private final String name;
    private final FileChannel channel;

    FileNio(String fileName, String mode) throws IOException {
        this.name = fileName;
        channel = new RandomAccessFile(fileName, mode).getChannel();
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        channel.position(newPosition);
        return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        try {
            return channel.write(src);
        } catch (NonWritableChannelException e) {
            throw new IOException("read only");
        }
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        long size = channel.size();
        if (newLength < size) {
            long pos = channel.position();
            channel.truncate(newLength);
            long newPos = channel.position();
            if (pos < newLength) {
                if (newPos != pos) {
                    channel.position(pos);
                }
            } else if (newPos > newLength) {
                //当截取长度小于原来的文件指针，将指针移至新文件的尾端
                channel.position(newLength);
            }
        }
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        channel.close();
    }



    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return super.tryLock(position, size, shared);
    }

    @Override
    public String toString(){
        return "nio"+name;
    }


}


