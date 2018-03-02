package priv.marionette.cache;

import priv.marionette.shell.FilePath;
import priv.marionette.shell.FilePathWrapper;
import priv.marionette.shell.FileSystemBase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

/**
 * 文件访问缓存
 *
 * @author Yue Yu
 * @create 2018-01-05 下午3:11
 **/
public class FilePathCache extends FilePathWrapper{

    /**
     * The instance.
     */
    public static final FilePathCache INSTANCE = new FilePathCache();

    /**
     * Register the file system.
     */
    static {
        FilePath.register(INSTANCE);
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileCache(getBase().open(mode));
    }

    @Override
    public String getScheme() {
        return "cache";
    }


    /**
     * A file with a read cache.
     */
    public static class FileCache extends FileSystemBase {

        private static final int CACHE_BLOCK_SIZE = 4 * 1024;
        private final FileChannel base;

        private final LIRSCache<ByteBuffer> cache;

        {
            LIRSCache.Config cc = new LIRSCache.Config();
            // 1 MB cache size
            cc.maxMemory = 1024 * 1024;
            cache = new LIRSCache<>(cc);
        }

        FileCache(FileChannel base) {
            this.base = base;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            base.close();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            base.position(newPosition);
            return this;
        }

        @Override
        public long position() throws IOException {
            return base.position();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return base.read(dst);
        }

        @Override
        public synchronized int read(ByteBuffer dst, long position) throws IOException {
            long cachePos = getCachePos(position);
            int off = (int) (position - cachePos);
            int len = CACHE_BLOCK_SIZE - off;
            len = Math.min(len, dst.remaining());
            ByteBuffer buff = cache.get(cachePos);
            if (buff == null) {
                buff = ByteBuffer.allocate(CACHE_BLOCK_SIZE);
                long pos = cachePos;
                while (true) {
                    int read = base.read(buff, pos);
                    if (read <= 0) {
                        break;
                    }
                    if (buff.remaining() == 0) {
                        break;
                    }
                    pos += read;
                }
                int read = buff.position();
                if (read == CACHE_BLOCK_SIZE) {
                    cache.put(cachePos, buff, CACHE_BLOCK_SIZE);
                } else {
                    if (read <= 0) {
                        return -1;
                    }
                    len = Math.min(len, read - off);
                }
            }
            dst.put(buff.array(), off, len);
            return len == 0 ? -1 : len;
        }

        private static long getCachePos(long pos) {
            return (pos / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
        }

        @Override
        public long size() throws IOException {
            return base.size();
        }

        @Override
        public synchronized FileChannel truncate(long newSize) throws IOException {
            cache.clear();
            base.truncate(newSize);
            return this;
        }

        @Override
        public synchronized int write(ByteBuffer src, long position) throws IOException {
            clearCache(src, position);
            return base.write(src, position);
        }

        @Override
        public synchronized int write(ByteBuffer src) throws IOException {
            clearCache(src, position());
            return base.write(src);
        }

        private void clearCache(ByteBuffer src, long position) {
            if (cache.size() > 0) {
                int len = src.remaining();
                long p = getCachePos(position);
                while (len > 0) {
                    cache.remove(p);
                    p += CACHE_BLOCK_SIZE;
                    len -= CACHE_BLOCK_SIZE;
                }
            }
        }

        @Override
        public void force(boolean metaData) throws IOException {
            base.force(metaData);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared)
                throws IOException {
            return base.tryLock(position, size, shared);
        }

        @Override
        public String toString() {
            return "cache:" + base.toString();
        }

    }


}
