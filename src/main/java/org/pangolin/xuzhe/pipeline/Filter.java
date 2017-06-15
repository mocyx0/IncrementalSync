package org.pangolin.xuzhe.pipeline;

import org.pangolin.yx.Config;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.pangolin.xuzhe.pipeline.Constants.STRING_LIST_SIZE;
import static org.pangolin.xuzhe.pipeline.StringArrayListPool.EMPTY_STRING_LIST;


/** TODO 统计一下在各个主键范围的个数，考虑使用位图索引作为PK set
 * Created by 29146 on 2017/6/13.
 */
public class Filter extends Thread {
    public static final BlockingQueue<ArrayList<String>> logStringListQueue = new ArrayBlockingQueue<ArrayList<String>>(100);
    public static final BlockingQueue<ArrayList<String>> dealtResult = new ArrayBlockingQueue<ArrayList<String>>(100);
    @Override
    public void run() {
        long beginPk = 600;
        long endPk = 700;
        BufferedWriter bw;
        try {

               bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Config.MIDDLE_HOME + "/hehe.txt")));

            StringArrayListPool stringArrayListPool = StringArrayListPool.getInstance();
            ArrayList<String> out = stringArrayListPool.get();
            ArrayList<String> storeResults = stringArrayListPool.get();
            ArrayList<String> readList = null;
            boolean flag = false;
            LogParser.updatePkSet(beginPk,endPk);
            while (true) {
                readList = logStringListQueue.take();
                if(readList == EMPTY_STRING_LIST) {
                    break;
                }
                for (String scanResult : readList) {
                    flag = LogParser.isBelongsToClient(scanResult, out);
                    if (flag == true) {
                         System.out.println(scanResult);
                        bw.write(scanResult + "\n");
                        if (storeResults.size() == STRING_LIST_SIZE) {
//                            dealtResult.put(storeResults);
                            stringArrayListPool.put(storeResults);
                            storeResults = stringArrayListPool.get();
                        }
                        storeResults.add(scanResult);
                    }
                    flag = false;
                }
                stringArrayListPool.put(readList);
                if (storeResults.size() != 0) {
//                    dealtResult.put(storeResults);
                    stringArrayListPool.put(storeResults);
                    storeResults = stringArrayListPool.get();
                }
            }
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
