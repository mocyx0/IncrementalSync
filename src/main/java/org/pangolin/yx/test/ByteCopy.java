package org.pangolin.yx.test;

/**
 * Created by yangxiao on 2017/6/19.
 */
public class ByteCopy {


    private static void testMmapRead() {
        byte[] bytesA = new byte[512 * 1024 * 1024];
        byte[] bytesB = new byte[512 * 1024 * 1024];
        int len = 1;
        for (int i = 0; i < bytesA.length; i++) {
            bytesA[i] = (byte) i;
            bytesB[i] = (byte) i;
        }
        long t1 = System.currentTimeMillis();
        int pos = 0;
        while (pos + len < bytesA.length) {
            System.arraycopy(bytesA, pos, bytesB, pos, len);
            pos += len;
        }
        long t2 = System.currentTimeMillis();
        pos = 0;
        while (pos + len < bytesA.length) {
            for (int i = 0; i < len; i++) {
                bytesB[pos + i] = bytesA[pos + i];
            }
            pos += len;
        }
        long t3 = System.currentTimeMillis();
        System.out.println(String.format("system:%d mannal:%d", t2 - t1, t3 - t2));
    }

    public static void main(String[] args) {
        try {
            testMmapRead();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
