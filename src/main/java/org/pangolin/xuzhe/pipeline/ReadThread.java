package org.pangolin.xuzhe.pipeline;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by 29146 on 2017/6/13.
 */
public class ReadThread extends  Thread{
    public static final BlockingQueue<ArrayList<String>> middleResult = new ArrayBlockingQueue<ArrayList<String>>(20);
    @Override
    public void run() {
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream("G:/研究生/AliCompetition/quarter-final/home/data/11.txt"));
            BufferedReader br = null;
            try {
                br = new BufferedReader(new InputStreamReader(bis,"gbk"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            ArrayList<String> al = new ArrayList<>();
            String s = null;
            try {
                while((s = br.readLine()) != null){

      //              System.out.println(s);
                    if(al.size() >= 10){
                        middleResult.offer(al);
                        al = new ArrayList<>();
                    }
                    al.add(s);
                }
                if(al.size() != 0){
                    middleResult.offer(al);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
