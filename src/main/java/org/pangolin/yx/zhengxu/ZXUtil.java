package org.pangolin.yx.zhengxu;

/**
 * Created by yangxiao on 2017/6/16.
 */
public class ZXUtil {

    //返回token的长度
    public static int nextToken(byte[] data, int off, char delimit) {
        int end = off;
        while (end < data.length) {
            if (data[end] == delimit) {
                break;
            }
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
