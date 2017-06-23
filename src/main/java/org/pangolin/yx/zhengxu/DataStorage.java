package org.pangolin.yx.zhengxu;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/17.
 */
public interface DataStorage {
    void doLog(LogRecord logRecord, byte[] data) throws Exception;

    void doLog(LogBlock logBlock, byte[] data, int logPos) throws Exception;
}

interface DataCollector {
    void writeBuffer(long id, ByteBuffer buffer) throws Exception;
}
