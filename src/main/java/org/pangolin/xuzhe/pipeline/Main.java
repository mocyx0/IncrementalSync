package org.pangolin.xuzhe.pipeline;

import java.util.ArrayList;

/**
 * Created by ubuntu on 17-6-13.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(new Client());
        t1.start();


    }
}
