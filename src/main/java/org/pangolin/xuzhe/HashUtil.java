package org.pangolin.xuzhe;

/**
 * Created by ubuntu on 17-6-5.
 */
public class HashUtil {
    public static int hash(byte[] data, int len) {
        int h = 0;
        byte val[] = data;

        for (int i = 0; i < len; i++) {
            h = 31 * h + val[i];
        }
        return h;
    }
}
