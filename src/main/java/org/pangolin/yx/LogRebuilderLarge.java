package org.pangolin.yx;


import com.alibaba.middleware.race.sync.Constants;

import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/11.
 */
public class LogRebuilderLarge {
    private static AliLogData aliLogData;

    public static int BUFFER_SIZE = 1024 * 1024;

    public static AtomicInteger sendSize = new AtomicInteger();

    public static void init(AliLogData aliLogData) {
        LogRebuilderLarge.aliLogData = aliLogData;

    }

    public static AtomicInteger outputCount = new AtomicInteger();

    private static class Worker implements Runnable {
        long start;//open
        long end;//close
        int index;
        long curId;
        AtomicInteger seq = new AtomicInteger();

        private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        ByteBuffer newBuffer = ByteBuffer.allocate(BUFFER_SIZE + 128);
        private HashMap<String, RandomAccessFile> rafs = new HashMap<>();

        private RandomAccessFile getLogFile(String path) throws Exception {
            if (!rafs.containsKey(path)) {
                rafs.put(path, new RandomAccessFile(path, "r"));
            }
            return rafs.get(path);
        }


        Worker(long start, long end, int index) {
            this.start = start;
            this.end = end;
            this.index = index;
            curId = start + 1;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    nextData();
                    if (buffer.limit() == 0) {
                        break;
                    } else {
                        newBuffer.clear();
                        //write index and limit
                        newBuffer.putInt(index);
                        newBuffer.putInt(seq.incrementAndGet());
                        newBuffer.putInt(buffer.limit());
                        newBuffer.put(buffer);
                        newBuffer.flip();
                        ResultWriter.writeBuffer(newBuffer);
                        //Config.serverLogger.info(String.format("channel send data index %d, seq %d,size %d, real size %d", index, seq.get(), buffer.limit(), newBuffer.limit()));
                        sendSize.addAndGet(newBuffer.limit());
                    }
                }
            } catch (Exception e) {
                Config.serverLogger.info("{}", e);
                System.exit(0);
            }
            latch.countDown();
        }

        private static ArrayList<String> getRecordData(ArrayList<LogRecord> logs) throws Exception {
            if (logs.size() == 0) {
                return new ArrayList<String>();
            }
            ArrayList<String> data = new ArrayList<>();
            TableInfo tinfo = aliLogData.tableInfo;
            int dataCount = tinfo.columns.size();
            HashMap<String, String> values = new HashMap<>();
            for (LogRecord v : logs) {
                if (values.size() != dataCount) {
                    //这里应该只会出现update->update->insert的结构,所以每个newvalue都是有意义的
                    for (LogColumnInfo colInfo : v.columns) {
                        if (!values.containsKey(colInfo.name)) {
                            values.put(colInfo.name, colInfo.newValue);
                        }
                    }
                } else {
                    break;
                }
            }
            if (values.size() == 0) {

            } else if (values.size() != dataCount) {
                throw new Exception("column count error");
            } else {
                for (String colName : tinfo.columns) {
                    data.add(values.get(colName));
                }
            }
            return data;
        }


        private void writeToBuffer(ByteBuffer bf, ArrayList<String> strs) {

            for (int i = 0; i < strs.size(); i++) {
                String s = strs.get(i);
                bf.put(s.getBytes());
                if (i != strs.size() - 1) {
                    bf.put((byte) '\t');
                }
            }
            if (strs.size() != 0) {
                bf.put((byte) '\n');
                outputCount.incrementAndGet();
            }
        }

        private void getRecord(long id) throws Exception {
            long targetId = id;
            long testId = id;
            int blockIndex = aliLogData.blockLogs.size() - 1;
            ArrayList<LogRecord> logs = new ArrayList<>();

            while (blockIndex >= 0) {
                BlockLog blockLog = aliLogData.blockLogs.get(blockIndex);
                if (targetId == -1) {
                    //deleted
                    break;
                } else {
                    LogOfTable logOfTable = blockLog.logOfTable;
                    if (logOfTable.isDeleted(targetId)) {
                        break;
                    }
                    LogRecord lastLog = logOfTable.getLogById(targetId);
                    while (lastLog != null) {
                        LogParser.fillFileInfo(lastLog, blockLog);
                        logs.add(lastLog);
                        //读取log信息
                        RandomAccessFile raf = getLogFile(lastLog.logPath);
                        String sline = Util.fillLogData(raf, lastLog);
                        if (lastLog.id != testId) {
                            Config.serverLogger.info(String.format("id not equal in %d  %s", testId, sline));
                            //System.exit();
                        }
                        testId = lastLog.preId;
                        if (lastLog.preLogOff != -1) {
                            lastLog = logOfTable.getLog(lastLog.preLogOff);
                        } else {
                            //这是单个block的第一条日志,上一条日志需要根据preid去上一个block查找
                            targetId = lastLog.preId;
                            break;
                        }
                    }
                }
                blockIndex--;
            }
            ArrayList<String> strs = getRecordData(logs);
            writeToBuffer(buffer, strs);
        }

        public void nextData() throws Exception {
            buffer.clear();
            while (curId <= end) {
                int oldPos = buffer.position();
                try {
                    getRecord(curId);
                    curId++;
                    /* 每行都输出  用于测试
                    if (buffer.position() != 0) {
                        break;
                    }
                    */
                } catch (BufferOverflowException e) {
                    buffer.position(oldPos);
                    break;
                }
            }
            buffer.flip();
        }
    }

    private static CountDownLatch latch;

    public static void run() throws Exception {
        int thCount = Config.CPU_COUNT;
        latch = new CountDownLatch(thCount);
        long len = Config.queryData.end - Config.queryData.start - 1;
        long blockLen = len / thCount;

        for (int i = 0; i < thCount; i++) {
            long s = Config.queryData.start + i * blockLen;
            long e = s + blockLen;
            if (i == thCount - 1) {
                e = Config.queryData.end - 1;
            }
            //index =0 表示结束
            Thread th = new Thread(new Worker(s, e, i + 1));
            th.start();

        }
        latch.await();
        ByteBuffer bf = ByteBuffer.allocate(16);
        bf.putInt(0);
        bf.flip();
        ResultWriter.writeBuffer(bf);
    }

}
