package org.pangolin.xuzhe.reformat;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.zip.Inflater;

import static org.pangolin.xuzhe.reformat.Constants.REDO_NUM;

/**
 * Created by XuZhe on 2017/6/25.
 */
public class UncompThread extends Thread {

    public BlockingQueue<byte[]> compressedBlockingQueue = new ArrayBlockingQueue<>(20);
    public BlockingQueue<byte[]>[] uncompBlockingQueue = new ArrayBlockingQueue[REDO_NUM];
    @Override
    public void run() {
        try {
            for(int i = 0; i < REDO_NUM; i++) {
                uncompBlockingQueue[i] = new ArrayBlockingQueue<byte[]>(20);
            }
            Inflater inflater = new Inflater();
            byte[] uncompBuf = new byte[1024*1024];
            while(true) {
                byte[] raw = compressedBlockingQueue.take();
                if(raw.length == 0) {
                    for(int i = 0; i < REDO_NUM; i++) {
                        uncompBlockingQueue[i].put(new byte[0]);
                    }
                    System.out.println("UncompThread Done!");
                    break;
                }
//                int size = Redo.uncompress(inflater, raw, 0, raw.length, uncompBuf, 0, uncompBuf.length);
                for(int i = 0; i < REDO_NUM; i++) {
//                    uncompBlockingQueue[i].put(Arrays.copyOf(uncompBuf, size));
                    uncompBlockingQueue[i].put(raw);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
