package org.pangolin.xuzhe.pipeline;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;



/**
 * Created by 29146 on 2017/6/13.
 */
public class Filter extends Thread {
    public static final BlockingQueue<ArrayList<String>> dealtResult = new ArrayBlockingQueue<ArrayList<String>>(100);
    @Override
    public void run() {
        long beginPk = 12;
        long endPk = 20;
        ArrayList<String> out = new ArrayList<>();
        ArrayList<String> storeResults = new ArrayList<>(6000);
        ArrayList<String> readList = null;
        boolean flag = false;
        while(true){
            try {
                readList = ReadThread.middleResult.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for(String scanResult : readList){
                flag = LogParser.isBelongsToClient(scanResult, out ,beginPk, endPk);
                if(flag == true){
                     System.out.println(scanResult);
                    if(storeResults.size() == 6000){
                        dealtResult.offer(storeResults);
                        storeResults = new ArrayList<>();
                    }
                    storeResults.add(scanResult);
                }
                flag = false;
            }
            if(storeResults.size() != 0){
                dealtResult.offer(storeResults);
                storeResults = new ArrayList<>();
            }
        }

    }

}
