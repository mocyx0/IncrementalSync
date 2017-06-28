package org.pangolin.yx.zhengxu;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/16.
 */
public class ZXUtil {


    public static volatile Unsafe unsafe;

    public static void init() throws Exception {
        Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
        singleoneInstanceField.setAccessible(true);
        unsafe = (Unsafe) singleoneInstanceField.get(null);
    }

    private int readInt(byte[] buffer, int off) {
        int re = (buffer[off] & 0xff)
                | ((buffer[off + 1] & 0xff) << 8)
                | ((buffer[off + 2] & 0xff) << 16)
                | ((buffer[off + 3] & 0xff) << 24);
        return re;
    }

    private long readLong(byte[] buf, int buffOff) {
        long re = (((long) buf[buffOff]) & 0xff)
                | (((long) buf[buffOff + 1] & 0xff) << 8)
                | (((long) buf[buffOff + 2] & 0xff) << 16)
                | (((long) buf[buffOff + 3] & 0xff) << 24)
                | (((long) buf[buffOff + 4] & 0xff) << 32)
                | (((long) buf[buffOff + 5] & 0xff) << 40)
                | (((long) buf[buffOff + 6] & 0xff) << 48)
                | (((long) buf[buffOff + 7] & 0xff) << 56);
        return re;
    }

    private void writeInt(byte[] buf, int buffOff, int v) {
        buf[buffOff] = (byte) (0xff & v);
        buf[buffOff + 1] = (byte) (0xff & v >>> 8);
        buf[buffOff + 2] = (byte) (0xff & v >>> 16);
        buf[buffOff + 3] = (byte) (0xff & v >>> 24);
    }

    private void writeLong(byte[] buf, int buffOff, long v) {
        buf[buffOff] = (byte) (0xff & v);
        buf[buffOff + 1] = (byte) (0xff & v >>> 8);
        buf[buffOff + 2] = (byte) (0xff & v >>> 16);
        buf[buffOff + 3] = (byte) (0xff & v >>> 24);

        buf[buffOff + 4] = (byte) (0xff & v >>> 32);
        buf[buffOff + 5] = (byte) (0xff & v >>> 40);
        buf[buffOff + 6] = (byte) (0xff & v >>> 48);
        buf[buffOff + 7] = (byte) (0xff & v >>> 56);
    }

    //返回token的长度
    public static final int nextToken(byte[] data, int off, char delimit) {
        int end = off;
        while (data[end] != delimit) {
            end++;
        }
        return end - off;
    }

    public static long parseLong(byte[] data, int off, int len) {
        long v = 0;
        for (int i = 0; i < len; i++) {
            v = v * 10 + (data[off + i] - '0');
        }
        return v;
    }
}
