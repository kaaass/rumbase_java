package net.kaaass.rumbase.record;

/**
 * MVCC常用操作函数库
 *
 * @author kaaass
 */
public class MvccUtil {

    /**
     * 将整数以大端序写入字节数组
     *
     * @param data   目的字节数组
     * @param offset 偏移
     * @param value  写入值
     */
    public static void writeInt(byte[] data, int offset, int value) {
        assert data.length >= 4;
        data[offset] = (byte) (value >> (8 * 3));
        data[offset + 1] = (byte) ((value >> (8 * 2)) & 0xff);
        data[offset + 2] = (byte) ((value >> 8) & 0xff);
        data[offset + 3] = (byte) (value & 0xff);
    }

    /**
     * 从字节数组读入大端序整数
     *
     * @param data   源字节数组
     * @param offset 偏移
     * @return 读入的整数
     */
    public static int readInt(byte[] data, int offset) {
        assert data.length >= 4;
        int result = 0;
        result |= (data[offset] & 0xff) << (8 * 3);
        result |= (data[offset + 1] & 0xff) << (8 * 2);
        result |= (data[offset + 2] & 0xff) << 8;
        result |= data[offset + 3] & 0xff;
        return result;
    }

    /**
     * 将长整数以大端序写入字节数组
     *
     * @param data   目的字节数组
     * @param offset 偏移
     * @param value  写入值
     */
    public static void writeLong(byte[] data, int offset, long value) {
        assert data.length >= 8;
        data[offset] = (byte) (value >> (8 * 7));
        data[offset + 1] = (byte) ((value >> (8 * 6)) & 0xff);
        data[offset + 2] = (byte) ((value >> (8 * 5)) & 0xff);
        data[offset + 3] = (byte) ((value >> (8 * 4)) & 0xff);
        data[offset + 4] = (byte) ((value >> (8 * 3)) & 0xff);
        data[offset + 5] = (byte) ((value >> (8 * 2)) & 0xff);
        data[offset + 6] = (byte) ((value >> 8) & 0xff);
        data[offset + 7] = (byte) (value & 0xff);
    }

    /**
     * 从字节数组读入大端序长整数
     *
     * @param data   源字节数组
     * @param offset 偏移
     * @return 读入的整数
     */
    public static long readLong(byte[] data, int offset) {
        assert data.length >= 8;
        long result = 0;
        result |= (long) (data[offset] & 0xff) << (8 * 7);
        result |= (long) (data[offset + 1] & 0xff) << (8 * 6);
        result |= (long) (data[offset + 2] & 0xff) << (8 * 5);
        result |= (long) (data[offset + 3] & 0xff) << (8 * 4);
        result |= (long) (data[offset + 4] & 0xff) << (8 * 3);
        result |= (long) (data[offset + 5] & 0xff) << (8 * 2);
        result |= (long) (data[offset + 6] & 0xff) << 8;
        result |= (long) data[offset + 7] & 0xff;
        return result;
    }
}
