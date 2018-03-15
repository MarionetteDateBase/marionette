package priv.marionette.ghost.type;

import priv.marionette.ghost.WriteBuffer;
import priv.marionette.tools.DataUtils;

import java.nio.ByteBuffer;

/**
 * 字符串数据类型
 *
 * @author Yue Yu
 * @create 2018-03-13 下午4:49
 **/
public class StringDataType implements  DataType{
    // 闭包实例
    public static final StringDataType INSTANCE = new StringDataType();

    @Override
    public int compare(Object a, Object b) {
        return a.toString().compareTo(b.toString());
    }

    @Override
    public int getMemory(Object obj) {
        return 24 + 2 * obj.toString().length();
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public String read(ByteBuffer buff) {
        int len = DataUtils.readVarInt(buff);
        return DataUtils.readString(buff, len);
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        String s = obj.toString();
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

}
