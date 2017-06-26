package org.pangolin.xuzhe.reformat;

import java.io.FileInputStream;
import java.io.IOException;

import static org.pangolin.xuzhe.reformat.Constants.REDO_NUM;
import static org.pangolin.xuzhe.reformat.Constants.UNCOMP_NUM;

/**
 * Created by XuZhe on 2017/6/25.
 */
public class CompFileReadingThread extends Thread {
    String fileName;
    UncompThread[] uncompThreads;
    Redo[] redos;
    public CompFileReadingThread(String fileName) {
        setName(this.getClass().getName());
        this.fileName = fileName;
        uncompThreads = new UncompThread[UNCOMP_NUM];
        for(int i = 0; i < uncompThreads.length; i++) {
            uncompThreads[i] = new UncompThread();
        }
        for(int i = 0; i < uncompThreads.length; i++) {
            uncompThreads[i].start();
        }
        redos = new Redo[REDO_NUM];
        for(int i = 0; i< REDO_NUM; i++) {
            redos[i] = new Redo(i, null);
            redos[i].start();
        }
    }

    @Override
    public void run() {
        try {
            long t1 = System.currentTimeMillis();
            FileInputStream inputStream = new FileInputStream(fileName);
            byte[] a = new byte[4];
            int index = 0;
            while(true) {
                int uncompThreadNo = index % uncompThreads.length;
                int readCnt = inputStream.read(a);
                if (readCnt == -1) {
                    for(int i = 0; i < uncompThreads.length; i++) {
                        uncompThreads[i].compressedBlockingQueue.put(new byte[0]);
                    }
                    break;
                }
                readCnt = a[0];
                readCnt = (readCnt << 8) | (a[1] & 0xFF);
                readCnt = (readCnt << 8) | (a[2] & 0xFF);
                readCnt = (readCnt << 8) | (a[3] & 0xFF);
                byte[] compressed = new byte[readCnt];
                inputStream.read(compressed);
                uncompThreads[uncompThreadNo].compressedBlockingQueue.put(compressed);
                index++;

            }
            for(int i = 0; i < uncompThreads.length; i++) {
                uncompThreads[i].join();
            }
            long t2 = System.currentTimeMillis();
            System.out.println("Uncomp Done! " + (t2-t1) + "ms");
            for(int i = 0; i < REDO_NUM; i++) {
                redos[i].join();
            }
            t2 = System.currentTimeMillis();
            System.out.println("Redo Done! " + (t2-t1) + "ms");
            byte[] result = new byte[50_000_000];
            int offset = 0;
            for(int i = 100001; i < 500_0000; i++) {
                for(int j = 0; j < redos.length; j++) {
                    int tmp = redos[j].getRecord(i, result, offset);

                    if (tmp != -1) {
                        offset = tmp;
                        break;
                    }
                }
            }
            t1 = System.currentTimeMillis();
            System.out.println(new String(result, 0, 200));
            System.out.printf("cnt:%d  time:%d ms", offset, t1-t2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        CompFileReadingThread t = new CompFileReadingThread("other/middle.dat2");
        t.start();
        t.join();
    }
}
