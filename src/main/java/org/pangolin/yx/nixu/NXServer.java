package org.pangolin.yx.nixu;

import org.pangolin.xuzhe.test.IOPerfTest;
import org.pangolin.xuzhe.test.ReadingThread;
import org.pangolin.yx.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/16.
 */
public class NXServer implements WorkerServer {

    @Override
    public void doTest() throws Exception {
        MLog.info("doTest start");

        //yx test
        LogParserTest.parseLog();

        IOPerfTest.positiveOrderReadByFileChannel(Config.DATA_HOME + "/1.txt");
        // 不读同一个文件，避免从pagecache读
        IOPerfTest.reverseOrderReadByFileChannel(Config.DATA_HOME + "/2.txt");
        String[] fileNameArray = new String[10];
        for (int i = 1; i <= 10; i++) {
            fileNameArray[i - 1] = String.format("%s/%d.txt", Config.DATA_HOME, i);
        }
        ReadingThread readingThread = new ReadingThread(fileNameArray);
        readingThread.start();
        readingThread.join();
        MLog.info("doTest done");
        //
        ByteBuffer bf = ByteBuffer.allocate(16);
        bf.putInt(0);
        bf.flip();
        ResultWriter.writeBuffer(bf);
    }

    @Override
    public void doData() throws Exception {

        // Thread.sleep(5000);

        if (Config.COPY_DATA) {
            ArrayList<String> files = Util.logFiles(Config.DATA_HOME);
            MLog.info("start copy");
            FileCopy.copyFile(files, Config.MIDDLE_HOME, false);
        }

        long t1 = System.currentTimeMillis();

        LogParser parser = new LogParser();
        //precache
        //PreCache.precache(Util.logFiles(Config.DATA_HOME));

        //等待precache一部分数据
        //Thread.sleep(Config.PRECACHE_DELAY);
        /*
        synchronized (PreCache.class) {
            PreCache.class.wait();
        }
        */
        // Thread.sleep(2000);
        //read log
        AliLogData data = parser.parseLog();
        MLog.info("parseLog done");
        MLog.info(String.format("linear hashing mem: %d", LinearHashing.TOTAL_MEM.get()));
        MLog.info(String.format("byte index mem: %d", LogOfTable.TOTAL_MEM.get()));
        MLog.info(String.format("insert %d udpate %d delete %d   pkUpdate %d", LogParser.insertCount.get(), LogParser.updateCount.get(), LogParser.deleteCount.get(), LogParser.pkUpdateCount.get()));
        //rebuild
        /*
        LogRebuilder rebuider = new LogRebuilder(data);
        RebuildResult result = rebuider.getResult();
        */

        //System.exit(0);
        long t2 = System.currentTimeMillis();
        LogRebuilderLarge.init(data);
        LogRebuilderLarge.run();
        long t3 = System.currentTimeMillis();
        MLog.info("rebuild done");
        MLog.info(String.format("cost time  index:%d   rebuild:%d", t2 - t1, t3 - t2));
        // logger.info(String.format("parse log count: %d", Util.parseLogCount.get()));
        MLog.info(String.format("read load count: %d", NXUtil.readLogCount.get()));
        MLog.info(String.format("out put line : %d,  byte %d", LogRebuilderLarge.outputCount.get(), LogRebuilderLarge.sendSize.get()));

    }
}
