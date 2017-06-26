package org.pangolin.xuzhe.reformat;

import java.nio.ByteBuffer;

/**
 * Created by XuZhe on 2017/6/24.
 */
public class MyDirectBuffer {
    ByteBuffer directBuffer;
    private final long baseAddr;
    public MyDirectBuffer(int capacity) {
        directBuffer = ByteBuffer.allocateDirect(capacity);
        baseAddr = UnsafeUtil.getAddress(directBuffer);
    }


}
