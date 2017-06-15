package org.pangolin.xuzhe.pipeline;

import org.junit.Test;

import java.util.ArrayList;

/**
 * Created by ubuntu on 17-6-15.
 */
public class SplitBlockAlgorithmTest {
    @Test
    public void test() {
        int[] fileSizeArray = new int[10];
        final int smallestSize = 1_066_899_590;
        for(int i = 0; i < fileSizeArray.length; i++) {
            fileSizeArray[i] = smallestSize + i;
        }
        final int workerCnt = 4;
        final int blockSize = 1<<20;
        ArrayList<Integer>[] readBlockNoList = new ArrayList[workerCnt];
        for(int i = 0; i < readBlockNoList.length; i++) {
            readBlockNoList[i] = new ArrayList<Integer>(500);
        }
        for(int workerNo = 0; workerNo < workerCnt; workerNo++) {
            long byteCnt = 0;
            int blockNo = 0;
            for(int fileNo = fileSizeArray.length; fileNo > 0; fileNo--) {
                byteCnt += fileSizeArray[fileNo-1];
                int blockCnt = (int)Math.round((double)fileSizeArray[fileNo-1]/blockSize);
                for(int j = blockCnt-1; j >= 0; j--) {
                    int no = blockNo%workerCnt;
                    if(no == workerNo) {
                        readBlockNoList[no].add(j);
                    }
                    blockNo++;
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("FileNo\t"));
        for(int workerNo = 1; workerNo <= workerCnt; workerNo++) {
            sb.append("Worker");
            sb.append(workerNo);
            sb.append("\t");
        }
        System.out.println(sb);

        sb = new StringBuilder();
        for(int i = 0; i < readBlockNoList[0].size(); i++) {
            sb.append("\t\t");
            for(int j = 0; j < workerCnt; j++) {
                if(readBlockNoList[j].size() < i+1) continue;
                sb.append(String.format("%4d", readBlockNoList[j].get(i)));
                sb.append("\t");
            }
            sb.append("\n");
        }
        System.out.println(sb);
    }
}
