package priv.marionette.security;

/**
 * 块加密接口定义类
 */
public interface BlockCipher {

    /**
     * 块的大小
     */
    int ALIGN = 16;

    /**
     * 用来加密的key，key的长度为16个字节
     *
     * @param key the key
     */
    void setKey(byte[] key);

    /**
     * 加密字节流
     *
     * @param bytes the byte array
     * @param off the start index
     * @param len the number of bytes to encrypt
     */
    void encrypt(byte[] bytes, int off, int len);

    /**
     * 解密字节流
     *
     * @param bytes the byte array
     * @param off the start index
     * @param len the number of bytes to decrypt
     */
    void decrypt(byte[] bytes, int off, int len);

    /**
     * 获取Random Key的长度
     *
     * @return the length of the key
     */
    int getKeyLength();

}
