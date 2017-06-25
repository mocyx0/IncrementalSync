package org.pangolin.yx.test;

/**
 * Created by yangxiao on 2017/6/25.
 */
public class MemoryWrite {

    private static int DATA_SIZE = 1024 * 1024 * 1024;

    private static void run() {
        byte[] datas = new byte[8];
        for (int i = 0; i < 8; i++) {
            datas[i] = (byte) i;
        }
        byte[] d1 = new byte[DATA_SIZE];
        char[] d2 = new char[DATA_SIZE / 2];
        long[] d3 = new long[DATA_SIZE / 8];

        int pos = 0;
        long t1 = System.currentTimeMillis();
        pos = 0;
        while (pos < d3.length) {
            d3[pos++] = (((long) datas[0] & 0xff)) |
                    (((long) datas[1] & 0xff) << 8) |
                    (((long) datas[2] & 0xff) << 16) |
                    (((long) datas[3] & 0xff) << 24) |

                    (((long) datas[4] & 0xff) << 32) |
                    (((long) datas[5] & 0xff) << 40) |
                    (((long) datas[6] & 0xff) << 48) |
                    (((long) datas[7] & 0xff) << 56)
            ;
        }

        long t2 = System.currentTimeMillis();
        pos = 0;
        while (pos < d2.length) {
            d2[pos++] = (char) ((((char) datas[0] & 0xff)) |
                    (((char) datas[1] & 0xff) << 8))
            ;
        }
        long t3 = System.currentTimeMillis();
        pos = 0;
        while (pos < d1.length) {
            for (int i = 0; i < datas.length; i++) {
                d1[pos++] = datas[i];
            }
            /*
            System.arraycopy(datas,0,d1,0,8);
            pos+=8;
            */
        }
        long t4 = System.currentTimeMillis();
        System.out.println(String.format("%d %d %d", t2 - t1, t3 - t2, t4 - t3));
    }

    public static void main(String[] args) {
        try {
            run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
