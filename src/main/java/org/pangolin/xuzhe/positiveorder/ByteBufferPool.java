package org.pangolin.xuzhe.positiveorder;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static org.pangolin.xuzhe.positiveorder.Constants.*;

/**
 * Created by 29146 on 2017/6/15.
 */
public class ByteBufferPool {
    private static ByteBufferPool instance = new ByteBufferPool();
    public static ByteBufferPool getInstance(){return instance;}

    private BlockingQueue<ByteBuffer> poolSmall = new LinkedBlockingDeque<>();
    private BlockingQueue<ByteBuffer> poolBigger = new LinkedBlockingDeque<>();
    private BlockingQueue<ByteBuffer> poolBiggest = new LinkedBlockingDeque<>();

//    private ByteBufferPool(){
//        while(poolSmall.size() < POOL_SIZE){
//            poolSmall.offer(ByteBuffer.allocate(BUFFER_SMALL));
//        }
//        while(poolBigger.size() < POOL_SIZE){
//            poolSmall.offer(ByteBuffer.allocate(BUFFER_BIGGER));
//        }
//        while(poolBiggest.size() < POOL_SIZE){
//            poolSmall.offer(ByteBuffer.allocate(BUFFER_BIGGEST));
//        }
//    }
//
//    public ByteBuffer getSmall() throws InterruptedException {
//        return poolSmall.take();
//    }
//    public ByteBuffer getBigger() throws InterruptedException {
//        return poolBigger.take();
//    }
//    public ByteBuffer getBiggest() throws InterruptedException {
//        return  poolBiggest.take();
//    }
////    public ByteBuffer get(int len) throws InterruptedException {
////        if(len <= BUFFER_SMALL){
////            return poolSmall.take();
////        }else if(len > BUFFER_SMALL && len <= BUFFER_BIGGER){
////            return poolBigger.take();
////        }else {
////            return  poolBiggest.take();
////        }
////    }
//
//    public void put(ByteBuffer buffer) throws InterruptedException {
//        buffer.clear();
//        if(buffer.capacity() == BUFFER_SMALL){
//            poolSmall.put(buffer);
//        }else if(buffer.capacity() == BUFFER_BIGGER){
//            poolBigger.put(buffer);
//        }else{
//            poolBiggest.put(buffer);
//        }
//    }

}
