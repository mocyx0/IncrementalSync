package org.pangolin.xuzhe.reformat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by XuZhe on 2017/6/25.
 */
public class DirectReadingThread extends Thread {

    private final String[] fileNameArray;

    public DirectReadingThread(String[] fileNameArray) {
        super("ReadingThread");
        this.fileNameArray = fileNameArray;
    }
    @Override
    public void run() {
        FileInputStream fis;
        try {
            long beginTime = System.currentTimeMillis();
            ByteBuffer currentBuffer = ByteBuffer.allocateDirect(512*1024);
            for (String fileName : fileNameArray) {
                System.out.println(fileName);
                fis = new FileInputStream(new File(fileName));
                FileChannel channel = fis.getChannel();
                while(true) {
                    int cnt = channel.read(currentBuffer);
                    currentBuffer.clear();
                    if(cnt == -1 || cnt == 0) {
                        break;
                    }
                }
            channel.close();
            fis.close();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Reading Done! " + (endTime-beginTime) + " ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws Exception {
//        String fileBaseName = "D:\\Code\\java\\中间件复赛\\test_data\\Downloads\\";
        String fileBaseName = "/root/ram/";
        int fileCnt = 0;
        for (int i = 1; i <= 10; i++) {
            String fileName = fileBaseName + i + ".txt";
            System.out.print("check file:" + fileName);
            File f = new File(fileName);
            if (f.exists()) {
                System.out.println(" exists");
                fileCnt++;
            } else {
                System.out.println(" not exists");

            }
        }
        String[] fileNames = new String[fileCnt];
        for (int i = 1; i <= fileCnt; i++) {
            fileNames[i - 1] = fileBaseName + i + ".txt";
        }
        long time1 = System.currentTimeMillis();
        DirectReadingThread readingThread = new DirectReadingThread(fileNames);
        readingThread.start();
        readingThread.join();
        long time2 = System.currentTimeMillis();
        System.out.println("elapsed time:" + (time2 - time1) + "ms");
    }
}
