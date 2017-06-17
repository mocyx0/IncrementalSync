package org.pangolin.yx.zhengxu;

import java.nio.ByteBuffer;

/**
 * Created by yangxiao on 2017/6/17.
 */
public interface DataStorage {
    void doLog(LogRecord logRecord) throws Exception;

    void writeBuffer(long id, ByteBuffer buffer) throws Exception;
}
