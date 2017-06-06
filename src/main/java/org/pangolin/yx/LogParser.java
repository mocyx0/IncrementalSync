package org.pangolin.yx;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/4.
 */

//表元信息
class TableInfo {
    String scheme;
    String table;
    String pk;
    //列名
    ArrayList<String> columns = new ArrayList<>();
}

class LogColumnInfo {
    String name;
    int type;
    String oldValue;
    String newValue;
    int isPk;
}

class StringParser {
    String str;
    int off;

    StringParser(String s, int off) {
        this.str = s;
        this.off = off;
    }

    boolean end() {
        return off >= str.length();
    }
}

class LogOfTable {
    HashMap<Long, LinkedList<LogRecord>> idToLogs = new HashMap<>();

    public void checkKey(Long id) {
        if (!idToLogs.containsKey(id)) {
            idToLogs.put(id, new LinkedList<LogRecord>());
        }
    }
}

class AliLogData {
    HashMap<String, TableInfo> tableInfos = new HashMap<>();
    ArrayList<BlockLog> blockLogs = new ArrayList<>();
}

class BlockLog {
    HashMap<String, TableInfo> tableInfos = new HashMap<>();
    HashMap<String, LogOfTable> logInfos = new HashMap<>();
    FileBlock fileBlock;
}

class FileBlock {
    String path;
    int index;
    long offset;
    int length;
}

class LogRecord {
    //
    public String opType;
    public int preId;
    public int id;
    //file info
    public long offset;
    public String logPath;
    public int length;
    //只有在rebuild时才会有数据
    public ArrayList<LogColumnInfo> columns = null;
}


public class LogParser {

    private static ConcurrentLinkedQueue<FileBlock> fileBlocks = new ConcurrentLinkedQueue<>();
    private static final AliLogData aliLogData = new AliLogData();
    private static AtomicInteger insertCount = new AtomicInteger();
    private static AtomicInteger updateCount = new AtomicInteger();
    private static AtomicInteger deleteCount = new AtomicInteger();

    private static void splitLogFile() {
        int fileIndex = 1;
        int blockIndex = 0;
        while (true) {
            String path = Config.DATA_HOME + "/" + fileIndex + ".txt";
            File file = new File(path);
            if (file.exists()) {
                long off = 0;
                while (off < file.length()) {
                    FileBlock newBlock = new FileBlock();
                    newBlock.path = path;
                    newBlock.index = blockIndex++;
                    newBlock.length = (int) Math.min(Config.BLOCK_SIZE, file.length() - off);
                    newBlock.offset = off;
                    off += Config.BLOCK_SIZE;
                    fileBlocks.add(newBlock);
                }
                fileIndex++;
            } else {
                break;
            }
        }
    }

    private static void parseLine(ReadLineInfo lineInfo, BlockLog blockLog) throws Exception {
        String line = lineInfo.line;

        StringParser parser = new StringParser(line, 0);
        String uid = Util.getNextToken(parser, '|');
        String time = Util.getNextToken(parser, '|');
        String scheme = Util.getNextToken(parser, '|');
        String table = Util.getNextToken(parser, '|');
        String op = Util.getNextToken(parser, '|');

        String hashKey = scheme + " " + table;
        //table的第一条insert记录包含所有列, 我们记录下元信息
        if (op.equals("I")) {
            if (!blockLog.tableInfos.containsKey(hashKey)) {
                int off = parser.off;
                TableInfo info = new TableInfo();
                info.scheme = scheme;
                info.table = table;
                LogColumnInfo cinfo = Util.getNextColumnInfo(parser);
                while (cinfo != null) {
                    info.columns.add(cinfo.name);
                    if (cinfo.isPk == 1) {
                        info.pk = cinfo.name;
                    }
                    cinfo = Util.getNextColumnInfo(parser);
                }
                blockLog.tableInfos.put(hashKey, info);
                parser.off = off;
            }

        }
        if (!blockLog.logInfos.containsKey(hashKey)) {
            blockLog.logInfos.put(hashKey, new LogOfTable());
        }

        //解析到主键为止
        LogColumnInfo cinfo = Util.getNextColumnInfo(parser);
        while (cinfo != null) {
            if (cinfo.isPk == 1) {
                break;
            }
            cinfo = Util.getNextColumnInfo(parser);
        }
        if (cinfo == null) {
            throw new Exception("no pk");
        }
        Long pkId = null;
        //根据操作的不同  获取对应的主键id
        if (op.equals("I")) {
            pkId = Long.parseLong(cinfo.newValue);
        } else if (op.equals("D")) {
            pkId = Long.parseLong(cinfo.oldValue);
        } else if (op.equals("U")) {
            pkId = Long.parseLong(cinfo.newValue);
        }


        blockLog.logInfos.get(hashKey).checkKey(pkId);
        LinkedList<LogRecord> logs = blockLog.logInfos.get(hashKey).idToLogs.get(pkId);

        LogRecord linfo = new LogRecord();
        linfo.opType = op;
        linfo.logPath = blockLog.fileBlock.path;
        linfo.offset = lineInfo.off;
        linfo.length = lineInfo.length;
        if (op.equals("U")) {
            linfo.id = Integer.parseInt(cinfo.newValue);
            linfo.preId = Integer.parseInt(cinfo.oldValue);
            updateCount.incrementAndGet();
        } else if (op.equals("I")) {
            linfo.id = Integer.parseInt(cinfo.newValue);
            insertCount.incrementAndGet();
        } else if (op.equals("D")) {
            linfo.id = Integer.parseInt(cinfo.oldValue);
            deleteCount.incrementAndGet();
        } else {
            throw new Exception("非法的操作类型");
        }
        //as a queue
        logs.push(linfo);
    }

    String logPath;


    private static class Worker implements Runnable {
        @Override
        public void run() {

            try {
                while (true) {
                    FileBlock block = fileBlocks.poll();
                    if (block == null) {
                        break;
                    } else {
                        BlockLog blockLog = parseLogBlock(block);
                        synchronized (aliLogData) {
                            aliLogData.blockLogs.add(blockLog);
                        }
                        latch.countDown();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

    private static CountDownLatch latch;


    private static BlockLog parseLogBlock(FileBlock block) throws Exception {
        BlockLog blockLog = new BlockLog();
        blockLog.fileBlock = block;
        LineReader lineReader = new LineReader(block.path, block.offset, block.length);
        ReadLineInfo line = lineReader.readLine();
        int lineIndex = 1;
        while (line.line != null) {
            parseLine(line, blockLog);
            line = lineReader.readLine();
            lineIndex++;
        }
        return blockLog;
    }

    private static void sortBlock() {
        //merge table Info
        for (BlockLog blockLog : aliLogData.blockLogs) {
            for (Map.Entry<String, TableInfo> entry : blockLog.tableInfos.entrySet()) {
                String k = entry.getKey();
                TableInfo v = entry.getValue();
                if (!aliLogData.tableInfos.containsKey(k)) {
                    aliLogData.tableInfos.put(k, v);
                }
            }
        }
        //sort by index
        Collections.sort(aliLogData.blockLogs, new Comparator<BlockLog>() {
            @Override
            public int compare(BlockLog o1, BlockLog o2) {
                return o1.fileBlock.index - o2.fileBlock.index;
            }
        });

    }

    public static AliLogData parseLog() throws Exception {

        splitLogFile();
        latch = new CountDownLatch(fileBlocks.size());
        int cpu = Runtime.getRuntime().availableProcessors();
        cpu = 1;
        for (int i = 0; i < cpu; i++) {
            Thread th = new Thread(new Worker());
            th.start();
        }
        latch.await();
        sortBlock();
        //

        return aliLogData;
//        logPath = Config.DATA_HOME + "/1.txt";
//        File f1 = new File(logPath);
//        lineReader = new LineReader(logPath, 0, f1.length());


    }
}
