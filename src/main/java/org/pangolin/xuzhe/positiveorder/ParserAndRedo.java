package org.pangolin.xuzhe.positiveorder;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.pangolin.xuzhe.positiveorder.Constants.LINE_MAX_LENGTH;
import static org.pangolin.xuzhe.positiveorder.Constants.PARSER_BLOCKING_QUEUE_SIZE;
import static org.pangolin.xuzhe.positiveorder.Constants.REDO_NUM;
import static org.pangolin.xuzhe.positiveorder.ReadBufferPool.EMPTY_BUFFER;

/**
 * Created by XuZhe on 2017/6/20.
 */
public class ParserAndRedo extends Thread {
    Logger logger = LoggerFactory.getLogger(Server.class);

    private BlockingQueue<ByteBuffer> buffers;
    private ArrayBlockingQueue<LogIndex>[] logIndexBlockingQueueArray;
    private int parserNo;
    public int readLineCnt = 0;
    public long readBytesCnt = 0;
    LogIndexPool logIndexPool;
    private Schema schema;

    public MyLong2IntHashMap pkMap = new MyLong2IntWithBitIndexHashMap(32-Integer.numberOfLeadingZeros(10000000/REDO_NUM), 0.99f);

    private byte[] dataSrc;
    private DataStore dataStore;
    private int databaseNameLen;
    private int tableNameLen;
    public ParserAndRedo(int parserNo) {
        this.setName("Parser" + parserNo);
        this.parserNo = parserNo;
        this.buffers = new ArrayBlockingQueue<ByteBuffer>(PARSER_BLOCKING_QUEUE_SIZE);
        logIndexBlockingQueueArray = new ArrayBlockingQueue[REDO_NUM];
        for(int i = 0; i < REDO_NUM; i++) {
            logIndexBlockingQueueArray[i] = new ArrayBlockingQueue<LogIndex>(PARSER_BLOCKING_QUEUE_SIZE);
        }

    }



    public LogIndex getLogIndexQueueHeaderByRedoId(int redoId) throws InterruptedException {
        LogIndex index =  logIndexBlockingQueueArray[redoId].take();
//		System.out.println(Thread.currentThread().getName() + " take a LogIndex");
        return index;
    }

    public void appendBuffer(ByteBuffer buffer) throws InterruptedException {
        this.buffers.put(buffer);
//		System.out.println("ReadingThread append a ReadBuffer");
    }

    @Override
    public void run() {
        ReadBufferPool pool = ReadBufferPool.getInstance();
        byte[] bytes = new byte[LINE_MAX_LENGTH];                     //一条日志最长有多少个字节？
        final byte newLine = (byte)'\n';
        try {
            ReadingThread.parserLatch.await();
            logIndexPool = LogIndexPool.getInstance();
            Schema schema = Schema.getInstance();
            databaseNameLen = schema.databaseNameLen;
            tableNameLen = schema.tableNameLen;
//            dataStore = new DataStore(schema.columnCount);
//            LogIndex logIndex;
//            while(true) {
//                ByteBuffer buffer = this.buffers.take();
////				logger.info("{} buffer.size:{}", getName(), buffers.size());
//                if(buffer == EMPTY_BUFFER) {
//                    for(int r = 0; r < REDO_NUM; r++) {
//                        logIndexBlockingQueueArray[r].put(LogIndex.EMPTY_LOG_INDEX);
//                    }
//                    break;
//                }
//
//                long begin = System.nanoTime();
//                process(buffer);
//
//                long end = System.nanoTime();
////				pool.put(buffer);
//            }
            //logger.info("{} done!", Thread.currentThread().getName());
        } catch (InterruptedException e) {
            logger.error("Worker was interrupted", e);
        } catch (Exception e) {
            logger.info("{}", e);
        }

    }

    private void process(ByteBuffer buffer) throws InterruptedException {
        int itemIndex = 0;
        int dataBegin = buffer.position();
        int dataEnd = buffer.limit();
//		int dataSize = (dataEnd - dataBegin);
//		int lastReadBytesCnt = readBytesCnt;
//		readBytesCnt += (dataEnd - dataBegin);
        int lineBegin = buffer.position();
        int lineEnd = -1;
        int subBegin = -1;
        byte[] data = buffer.array();
        int op = 0; //byte
        long oldPK = -1, newPK = -1;
        int hash = 0;
        int i = 0;
        int b; //byte
        LogIndex logIndex = logIndexPool.get();
        logIndex.setByteBuffer(buffer);

        int logItemIndex = 0;
        // 以 |mysql-bin.00001717148769|1496736165000|middleware3|student|I|id:1:1|NULL|11
        // |first_name:2:0|NULL|阮|last_name:2:0|NULL|甲|sex:2:0|NULL|女|score:1:0|NULL|53|
        for (i = dataBegin; i < dataEnd; ++i) {
            b = data[i];
//			char ch = (char) b;
            if (b == '|') {
                ++itemIndex;
                if(itemIndex == 1) {
                    i += 18;
                } else if(itemIndex == 2) {
//                    System.out.println(new String(data, i, 13 + 1 + 1 + databaseNameLen + tableNameLen));
                    i += (13 + 1 + 1 + databaseNameLen + tableNameLen);
                    itemIndex = 4;
//                    break;
                } else if (itemIndex == 5) {
                    ++i;
//                    System.out.println(new String(data, i, 100));
                    op = data[i];//I|id:1:1|NULL|11|first_na...

                    if (op != 'I' && op != 'U' && op != 'D')
                        throw new RuntimeException("op error:" + (char) op + ", raw:" + new String(data, lineBegin, i+20-lineBegin));
                    // 直接跳过  id:1:1
                    i += 9; // after:  NULL|11|first_na..
                    if (op == 'I') {
                        oldPK = -1;
                        i += 5;  // after: 11|first_name:2:0|NULL|阮
                    } else {
//						oldPK = data[i++];
                        oldPK = 0;
                        while ((b = data[i]) != '|') {
                            oldPK = oldPK * 10 + (b - '0');
                            ++i;
                        }
                        ++i;
                    }
                    if(op == 'D') {
                        newPK = -1;
                    } else {
                        newPK = 0;
                        while ((b = data[i]) != '|') {
                            newPK = newPK * 10 + (b - '0');
                            ++i;
                        }
                    }
//					if(oldPK != -1 && oldPK != newPK) {
//						System.out.println(oldPK);
//					}
//					if( oldPK != -1 && newPK != -1 &&(oldPK < 0 || newPK < 0))
//						System.out.println();
                    --i; // after:
                    itemIndex += 3;
//					String s = new String(data, lineBegin, i-lineBegin+1);
                    logIndex.addNewLog(oldPK, newPK, op, logItemIndex);
                } else if (itemIndex == 9) {
                    // current: |first_name:2:0|NULL|阮|...
                    // 除PK以外，第一列的开始
//					buffer.position(i);
//					printSubLine(buffer);
                    int[] hashs = logIndex.getHashColumnName(logItemIndex);
                    int[] columnNewValues = logIndex.getColumnNewValues(logItemIndex);
                    int[] columnValueLens = logIndex.getColumnValueLens(logItemIndex);
                    int columnIndex = 0;
                    while(data[i+1] != '\n') {
                        // 计算column name的hash code
                        ++i;
                        hash = 0;
                        while ((b = data[i]) != ':') {
                            hash = 31 * hash + b;
//						System.out.print((char)b);
                            ++i;
                        } // after: :2:0|NULL|阮|...
//					System.out.println();
                        hashs[columnIndex] = schema.columnHash2NoMap.get(hash);

                        i += 4; // after: |NULL|阮|last_name:2:0...
                        // 开始解析列的 新旧值
                        if (op == 'I') {
                            i += 5; // 插入操作的旧值为null，直接跳过
                            // after: |阮|last_name:2:0...
                        } else {
                            ++i;
                            int oldValueBegin = i;
                            while ((b = data[i]) != '|') {
                                ++i;
                            }
                            int oldValueEnd = i;

                        }
                        ++i; // after: 阮|last_name:2:0...
                        int newValueBegin = i;
                        while ((b = data[i]) != '|') {
                            ++i;
                        }
                        int newValueEnd = i;
                        columnNewValues[columnIndex] = newValueBegin;
                        columnValueLens[columnIndex] = (short)(newValueEnd-newValueBegin);
                        ++columnIndex;
                    }
                    logIndex.setColumnSize(logItemIndex, columnIndex);
                } else {

                }
            } else if (b == '\n') {
                // 处理完一行了
                lineEnd = i;
                ++readLineCnt;
                readBytesCnt += (lineEnd - lineBegin + 1);
                lineBegin = lineEnd + 1;
                itemIndex = 0;
                ++logItemIndex;
            }
        }
        logIndex.setLogSize(logItemIndex);
        dataSrc = logIndex.getByteBuffer().array();
        try {
            long[] oldPKs = logIndex.getOldPks();
            for (int j = 0; j < logIndex.getLogSize(); j++) {
                int logType = logIndex.getLogType(j);
                if (logType == 'I') {
                    long newPk =logIndex.getNewPk(j);

//                    if(newPk % REDO_NUM == this.redoId) {
                        insertResualt(pkMap, logIndex, j, newPk);
//                    }
                } else if (logType == 'U') {
                    updateResualt(pkMap, logIndex, j, oldPKs[j]);
                } else {
                    deleteResualt(pkMap, logIndex, oldPKs[j]);
                }
            }

//                    System.out.println("LogIndex Done!" + count);
        } finally {
            logIndex.release();
        }

    }

    private void printSubLine(ByteBuffer buffer) {
        int mark = buffer.position();
        byte[] subRaw = new byte[50];
        buffer.get(subRaw);
        buffer.position(mark);
        String s2 = new String(subRaw);
        System.out.println(s2);
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public int getParserNo() {
        return parserNo;
    }

    private void deleteResualt(MyLong2IntHashMap pkMap, LogIndex logIndex, long oldPk) throws InterruptedException {
        int indexInStore = pkMap.remove(oldPk);
        if(indexInStore == -1) {

        } else {
            dataStore.deleteRecord(indexInStore);
        }
    }

    private void updateResualt(MyLong2IntHashMap pkMap, LogIndex logIndex, int index, long oldPk) throws InterruptedException {
        int recordIndexInStore = pkMap.get(oldPk);
        if (recordIndexInStore == -1) {
            return;
        }
        long newPk = logIndex.getNewPk(index);
        int[] newValues = logIndex.getColumnNewValues(index);
        int[] names = logIndex.getHashColumnName(index);
        int[] valueLens = logIndex.getColumnValueLens(index);
        int columnSize = logIndex.getColumnSize(index);
        for (int i = 0; i < columnSize; i++) {
            int columnIndex = names[i];
            int columnPos = newValues[i];
            int columnLen = valueLens[i];
//            dataStore.updateRecord(recordIndexInStore, columnIndex, dataSrc, columnPos, columnLen);
        }
        if (oldPk != newPk) {
            pkMap.remove(oldPk);
            pkMap.put(newPk, recordIndexInStore);
        }
    }

    private void insertResualt(MyLong2IntHashMap pkMap, LogIndex logIndex, int index, long newPk) throws InterruptedException {
        //每个pk对应到不同的线程
        int columnSize = logIndex.getColumnSize(index);
//        int indexInStore = dataStore.createRecord();
//        for (int i = 0; i < columnSize; i++) {
//            int columnIndex = logIndex.getHashColumnName(index)[i];
//            int columnPos = logIndex.getColumnNewValues(index)[i];
//            int columnLen = logIndex.getColumnValueLens(index)[i];
//            dataStore.updateRecord(indexInStore, columnIndex, dataSrc, columnPos, columnLen);
//
//        }
//        pkMap.put(newPk, indexInStore);
    }

    public int getRecord(long pk, byte[] out, int pos) {
        int indexInStore = pkMap.get(pk);
        if(indexInStore == -1) {
            return -1;
        }
        return this.dataStore.getRecord(indexInStore, out, pos);
    }
}
