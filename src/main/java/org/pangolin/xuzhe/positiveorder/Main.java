package org.pangolin.xuzhe.positiveorder;


import static org.pangolin.xuzhe.positiveorder.Constants.*;

/**
 * Created by ubuntu on 17-6-13.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        for(int i = 0 ; i < REDO_NUM; i++){
            Thread  t1=  new Thread(new Redo(1234, 5242),"thread" + i);
            t1.start();
        }

    }
}
