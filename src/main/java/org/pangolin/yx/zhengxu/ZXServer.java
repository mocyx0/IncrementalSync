package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;
import org.pangolin.yx.Util;
import org.pangolin.yx.WorkerServer;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/16.
 */
public class ZXServer implements WorkerServer {

    private Logger logger;

    public ZXServer() {
        logger = Config.serverLogger;
    }

    @Override
    public void doTest() throws Exception {

    }

    @Override
    public void doData() throws Exception {

        ArrayList<String> paths = Util.logFiles(Config.DATA_HOME);
        LineReader lineReader = new LineReader(paths);
        LineInfo lineInfo = lineReader.nextLine();
        long line = 0;
        while (lineInfo != null) {
            line++;
//            String s = new String(lineInfo.data);
            //System.out.println(s);
            LogRecord logRecord = LineParser.parseLine(lineInfo);
            lineInfo = lineReader.nextLine();
        }
        logger.info(String.format("parse end, line:%d", line));

    }
}
