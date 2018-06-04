package priv.marionette.compress;

import priv.marionette.Exception.DbException;
import priv.marionette.api.ErrorCode;

import java.util.StringTokenizer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * jdk默认压缩算法封装类
 */
public class CompressDeflate implements Compressor {

    private int level = Deflater.DEFAULT_COMPRESSION;
    private int strategy = Deflater.DEFAULT_STRATEGY;

    @Override
    public void setOptions(String options) {
        if (options == null) {
            return;
        }
        try {
            StringTokenizer tokenizer = new StringTokenizer(options);
            while (tokenizer.hasMoreElements()) {
                String option = tokenizer.nextToken();
                if ("level".equals(option) || "l".equals(option)) {
                    level = Integer.parseInt(tokenizer.nextToken());
                } else if ("strategy".equals(option) || "s".equals(option)) {
                    strategy = Integer.parseInt(tokenizer.nextToken());
                }
                Deflater deflater = new Deflater(level);
                deflater.setStrategy(strategy);
            }
        } catch (Exception e) {
            throw DbException.get(ErrorCode.UNSUPPORTED_COMPRESSION_OPTIONS_1, options);
        }
    }

    @Override
    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        Deflater deflater = new Deflater(level);
        deflater.setStrategy(strategy);
        deflater.setInput(in, 0, inLen);
        deflater.finish();
        int compressed = deflater.deflate(out, outPos, out.length - outPos);
        while (compressed == 0) {
            // the compressed length is 0, meaning compression didn't work
            // (sounds like a JDK bug)
            // try again, using the default strategy and compression level
            strategy = Deflater.DEFAULT_STRATEGY;
            level = Deflater.DEFAULT_COMPRESSION;
            return compress(in, inLen, out, outPos);
        }
        deflater.end();
        return outPos + compressed;
    }

    @Override
    public int getAlgorithm() {
        return Compressor.DEFLATE;
    }

    @Override
    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos,
            int outLen) {
        Inflater decompresser = new Inflater();
        decompresser.setInput(in, inPos, inLen);
        decompresser.finished();
        try {
            int len = decompresser.inflate(out, outPos, outLen);
            if (len != outLen) {
                throw new DataFormatException(len + " " + outLen);
            }
        } catch (DataFormatException e) {
            throw DbException.get(ErrorCode.COMPRESSION_ERROR, e);
        }
        decompresser.end();
    }

}
