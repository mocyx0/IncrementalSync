package org.pangolin.xuzhe.pipeline;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.pangolin.xuzhe.pipeline.Constants.BUFFER_SIZE;

/**
 * Created by 29146 on 2017/6/14.
 */
public class Client extends Thread {
    @Override
    public void run() {
        int beginPk = 600;
        int endPk = 700;
        //存放最后需要排序的Record
        Record[] rSet = new Record[endPk - beginPk];
        //每个Client线程创建一个redo对象用于重做
        Redo redo = new Redo(beginPk, endPk);
        //用于将 rSet转为String[]输出
        String result[] = null;
        Record r = null;
        int len = -1;
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream("G:/研究生/AliCompetition/quarter-final/home/data/hehe.txt"));
            BufferedReader br = null;
            br = new BufferedReader(new InputStreamReader(bis, "utf-8"));
            String s = null;
 //           while (true) {
                while ((s = br.readLine()) != null) {
                    //核心重做部分
                    r = redo.redo(s);
                    if (r != null) {
                        rSet[++len] = r;
                    }
                    r = null;
                }
                //对rset进行堆排序
                HeapSort.heapSorting(rSet, len + 1);
                for (int i = 0; i < len + 1; i++) {
                    r = rSet[i];
                    result = r.updateInsertInfo(r.getLog());
                    for (String hehe : result) {
                            System.out.print(hehe + " ");
                    }
                    System.out.println();
                }
 //           }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
