package org.pangolin.yx.zhengxu;

import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/18.
 * 解析日志并将日志传入对应的queue
 */
public interface FileParser {
    void run(LogQueues queues);
}
