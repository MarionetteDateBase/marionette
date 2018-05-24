package priv.marionette.compress;

import java.nio.ByteBuffer;

/**
 * 这个类实现了Apple的LZF算法(无损压缩算法Lempel-Ziv的变体)，时间复杂度优先的算法
 *
 * 使用要点：
 * 1.每个instance并非线程安全的
 * 2.The data buffers应该小于1GB
 * 3.出于性能考虑，扩展性的安全检查被忽略
 * 4.无效的压缩数据将会导致一个ArrayIndexOutOfBoundsException
 *
 * LZF的压缩形式有以下两种:
 * 1.Literal run: 直接将字节码从input拷贝至output
 * 2.Back-reference: 拷贝当前offset前length长度的字节码至output stream，length至少三个字节长度
 *
 * 压缩流的第一个字节是控制字节。
 * 在常规模式下，控制字节的最高三位置0，非有效位，后五位为常规模式下的stream长度。
 * 在回朔引用模式下，控制字节的最高三位表示回朔长度，下一个字节表示stream长度。
 *
 */
public final class CompressLZF implements Compressor {

    /**
     * 哈希表中的entries数量
     */
    private static final int HASH_SIZE = 1 << 14;

    /**
     * 一个chunk中的字面量的最大值
     */
    private static final int MAX_LITERAL = 1 << 5;

    /**
     * 回朔引用所允许的最大offset(8192).
     */
    private static final int MAX_OFF = 1 << 13;

    /**
     *  回朔引用的最大长度(264).
     */
    private static final int MAX_REF = (1 << 8) + (1 << 3);

    private int[] cachedHashTable;

    @Override
    public void setOptions(String options) {
    }

    /**
     * 返回的整型数前两个字节为0x00,后两个字节为index和index+1位置的字节
     */
    private static int first(byte[] in, int inPos) {
        return (in[inPos] << 8) | (in[inPos + 1] & 255);
    }

    /**
     * 返回的整型数前两个字节为0x00,后两个字节为index和index+1位置的字节
     */
    private static int first(ByteBuffer in, int inPos) {
        return (in.get(inPos) << 8) | (in.get(inPos + 1) & 255);
    }

    /**
     * 将value向左偏移一个字节，并将这个字节添加至inPos+2
     */
    private static int next(int v, byte[] in, int inPos) {
        return (v << 8) | (in[inPos + 2] & 255);
    }

    /**
     * 将value向左偏移一个字节，并将这个字节添加至inPos+2
     */
    private static int next(int v, ByteBuffer in, int inPos) {
        return (v << 8) | (in.get(inPos + 2) & 255);
    }

    /**
     * 计算在哈希表中的地址
     */
    private static int hash(int h) {
        return ((h * 2777) >> 9) & (HASH_SIZE - 1);
    }

    @Override
    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        int inPos = 0;
        if (cachedHashTable == null) {
            cachedHashTable = new int[HASH_SIZE];
        }
        int[] hashTab = cachedHashTable;
        int literals = 0;
        outPos++;
        int future = first(in, 0);
        while (inPos < inLen - 4) {
            byte p2 = in[inPos + 2];
            future = (future << 8) + (p2 & 255);
            int off = hash(future);
            int ref = hashTab[off];
            hashTab[off] = inPos;
            if (ref < inPos
                        && ref > 0
                        && (off = inPos - ref - 1) < MAX_OFF
                        && in[ref + 2] == p2
                        && in[ref + 1] == (byte) (future >> 8)
                        && in[ref] == (byte) (future >> 16)) {
                int maxLen = inLen - inPos - 2;
                if (maxLen > MAX_REF) {
                    maxLen = MAX_REF;
                }
                if (literals == 0) {
                    outPos--;
                } else {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                }
                int len = 3;
                while (len < maxLen && in[ref + len] == in[inPos + len]) {
                    len++;
                }
                len -= 2;
                if (len < 7) {
                    out[outPos++] = (byte) ((off >> 8) + (len << 5));
                } else {
                    out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                    out[outPos++] = (byte) (len - 7);
                }
                out[outPos++] = (byte) off;
                outPos++;
                inPos += len;
                future = first(in, inPos);
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
            } else {
                out[outPos++] = in[inPos++];
                literals++;
                if (literals == MAX_LITERAL) {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                    outPos++;
                }
            }
        }
        while (inPos < inLen) {
            out[outPos++] = in[inPos++];
            literals++;
            if (literals == MAX_LITERAL) {
                out[outPos - literals - 1] = (byte) (literals - 1);
                literals = 0;
                outPos++;
            }
        }
        out[outPos - literals - 1] = (byte) (literals - 1);
        if (literals == 0) {
            outPos--;
        }
        return outPos;
    }

    /**
     * 压缩字节流
     *
     * @param in the input data
     * @param inPos the offset at the input buffer
     * @param out the output area
     * @param outPos the offset at the output array
     * @return the end position
     */
    public int compress(ByteBuffer in, int inPos, byte[] out, int outPos) {
        int inLen = in.capacity() - inPos;
        if (cachedHashTable == null) {
            cachedHashTable = new int[HASH_SIZE];
        }
        int[] hashTab = cachedHashTable;
        int literals = 0;
        outPos++;
        int future = first(in, 0);
        while (inPos < inLen - 4) {
            byte p2 = in.get(inPos + 2);
            future = (future << 8) + (p2 & 255);
            int off = hash(future);
            int ref = hashTab[off];
            hashTab[off] = inPos;
            if (ref < inPos
                        && ref > 0
                        && (off = inPos - ref - 1) < MAX_OFF
                        && in.get(ref + 2) == p2
                        && in.get(ref + 1) == (byte) (future >> 8)
                        && in.get(ref) == (byte) (future >> 16)) {
                int maxLen = inLen - inPos - 2;
                if (maxLen > MAX_REF) {
                    maxLen = MAX_REF;
                }
                if (literals == 0) {
                    outPos--;
                } else {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                }
                int len = 3;
                while (len < maxLen && in.get(ref + len) == in.get(inPos + len)) {
                    len++;
                }
                len -= 2;
                if (len < 7) {
                    out[outPos++] = (byte) ((off >> 8) + (len << 5));
                } else {
                    out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                    out[outPos++] = (byte) (len - 7);
                }
                out[outPos++] = (byte) off;
                outPos++;
                inPos += len;
                future = first(in, inPos);
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
            } else {
                out[outPos++] = in.get(inPos++);
                literals++;
                if (literals == MAX_LITERAL) {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                    outPos++;
                }
            }
        }

        while (inPos < inLen) {
            out[outPos++] = in.get(inPos++);
            literals++;
            if (literals == MAX_LITERAL) {
                out[outPos - literals - 1] = (byte) (literals - 1);
                literals = 0;
                outPos++;
            }
        }

        out[outPos - literals - 1] = (byte) (literals - 1);
        if (literals == 0) {
            outPos--;
        }
        return outPos;
    }

    @Override
    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos,
            int outLen) {

        if (inPos < 0 || outPos < 0 || outLen < 0) {
            throw new IllegalArgumentException();
        }
        do {
            int ctrl = in[inPos++] & 255;
            if (ctrl < MAX_LITERAL) {
                ctrl++;
                System.arraycopy(in, inPos, out, outPos, ctrl);
                outPos += ctrl;
                inPos += ctrl;
            } else {
                int len = ctrl >> 5;
                if (len == 7) {
                    len += in[inPos++] & 255;
                }
                len += 2;

                ctrl = -((ctrl & 0x1f) << 8) - 1;

                ctrl -= in[inPos++] & 255;

                ctrl += outPos;
                if (outPos + len >= out.length) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                for (int i = 0; i < len; i++) {
                    out[outPos++] = out[ctrl++];
                }
            }
        } while (outPos < outLen);
    }


    public static void expand(ByteBuffer in, ByteBuffer out) {
        do {
            int ctrl = in.get() & 255;
            if (ctrl < MAX_LITERAL) {
                ctrl++;
                for (int i = 0; i < ctrl; i++) {
                    out.put(in.get());
                }
            } else {
                int len = ctrl >> 5;
                if (len == 7) {
                    len += in.get() & 255;
                }
                len += 2;

                ctrl = -((ctrl & 0x1f) << 8) - 1;

                ctrl -= in.get() & 255;

                ctrl += out.position();
                for (int i = 0; i < len; i++) {
                    out.put(out.get(ctrl++));
                }
            }
        } while (out.position() < out.capacity());
    }

    @Override
    public int getAlgorithm() {
        return Compressor.LZF;
    }

}