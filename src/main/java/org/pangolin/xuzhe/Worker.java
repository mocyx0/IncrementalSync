package org.pangolin.xuzhe;

import org.pangolin.yx.MLog;

import static org.pangolin.xuzhe.Constants.LINE_MAX_LENGTH;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by ubuntu on 17-6-3.
 */
public class Worker extends Thread {
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
//    public static final TreeMap<Long, LogMessage> firstScan = new TreeMap<>(new Comparator<Long>(){
//		@Override
//		public int compare(Long o1, Long o2) {
//			return o2.compareTo(o1);
//		}	
//	});
    String schema = "middleware3";
    String table = "student";
    long beginId = 5000;
    long endId = 10000;
    byte[] schemaByte = schema.getBytes();
    int schemaByteLen = schemaByte.length;
    byte[] tableByte = table.getBytes();
    int tableByteLen = tableByte.length;
    public static final Map<Long, TreeMap<Long,String>> firstScan = new ConcurrentHashMap<>();
    private BlockingQueue<Block> buffers;
    private static AtomicInteger workerNum = new AtomicInteger(0);
    public Worker() {
        super("Worker" + workerNum.incrementAndGet());
        this.buffers = new ArrayBlockingQueue<Block>(20, true);

//    	try {
//    		String str = new String(bytes, 0, len+1, "utf-8");
//    		System.out.println(str);
//    		return;
//    	} catch (UnsupportedEncodingException e) {
//			// TODO: handle exception
//		}

    }

    public void appendBuffer(ByteBuffer buffer, long pos) throws InterruptedException {
    	Block block = new Block(buffer, pos);
        this.buffers.put(block);
    }
    
	@Override
    public void run() {
        ByteBufferPool pool = ByteBufferPool.getInstance();
        byte[] bytes = new byte[LINE_MAX_LENGTH];                     //一条日志最长有多少个字节？
        final byte newLine = (byte)'\n';
        try {
//          	int ggeg = 0;
            while(true) {    	
//              	if(ggeg >1)break;
            	Block block = this.buffers.take();
            	ByteBuffer buffer = block.buffer;
                if(buffer == EMPTY_BUFFER) break;
                long begin = System.nanoTime();
                int len = -1;
                int currentPos = 0;
                long position = block.getPosition();
                int fileNo = (int)((position & (long)(Math.pow(2, 63) + Math.pow(2, 62) + Math.pow(2, 61) + Math.pow(2, 60))) >> 56);
                long  blockBeginPosition = position & ((long)Math.pow(2, 60) - 1);                
                //两种情况：1.buffer中的数据块可能包含半条日志（涉及到读取数据块的顺序性问题）(已解决)
//                  int hehe = 0;
                while(buffer.hasRemaining()) {
//                 	if(hehe > 100)break;
                    byte b = buffer.get();
                    if(b != newLine) {
                        bytes[++len] = b;
                    } else {
 //                 	hehe++;
 //                   	System.out.println((blockBeginPosition + currentPos) + "----" + (len + 1));
                         process(bytes, fileNo, blockBeginPosition + currentPos, len + 1);
                        currentPos = currentPos + len + 2;
                        len = -1;
                    }
                }
                long end = System.nanoTime();
                // 将日志注释会加速处理速度
//              logger.info("{} process elapsed time:{} ns", Thread.currentThread().getName(), (end-begin));
                pool.put(buffer);
   //               ggeg++;
            }
            
            validate();
        } catch (InterruptedException e) {
            MLog.info("Worker was interrupted "+ e.toString());
        }

    }
	
	private void validate(){
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(new File("data/data_example.txt"));
			FileChannel channel = fis.getChannel();
			ByteBuffer byteBuffer = ByteBuffer.allocate(200);
			for(Entry<Long, TreeMap<Long,String>> idSet: firstScan.entrySet()){
				System.out.println("id为" + idSet.getKey() + "的相关日志记录有:");
				for(Entry<Long, String> timeSet : idSet.getValue().entrySet()){
 				 	String str = timeSet.getValue();
					String[] split1 = str.split(":");
					String[] split2 = split1[1].split(",");
					long currentPos = Long.parseLong(split2[3]);
					int len = Integer.parseInt(split2[4]);
					channel.read(byteBuffer,currentPos);
					System.out.println(new String(byteBuffer.array(),0,len));
					byteBuffer.clear();
				}
			}
			channel.close();
			fis.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    private void process(byte[] bytes, int fileNo, long currentPos, int len) {

        ByteBuffer bytebuffer = ByteBuffer.wrap(bytes, 0, len);
    	bytebuffer.get();
    	findNext(bytebuffer);
    	
    	//回溯查找时间戳字段使用
    	int beginTimePos = bytebuffer.position();
    	
    	findNext(bytebuffer);
    	
    	if( compareByte(bytebuffer, schemaByte, schemaByteLen)== '|'){
    		if(compareByte(bytebuffer, tableByte, tableByteLen) == '|'){
    			//获取该条日志的类型
    			byte logType = bytebuffer.get();
    			bytebuffer.get();
    			findNext(bytebuffer);
    			//读取id的更新前的值，更新后的值
    			long preUpdate =  getFields(bytebuffer);
    			long nextUpdate = getFields(bytebuffer);
    			//如果是更新的话，获取其所有更新字段
    			String updateFields = "-1";
    			if(logType == (byte)'U'){
    				updateFields = getUpdateFields(bytebuffer);
    			}
    			//读取时间戳
    			bytebuffer.position(beginTimePos);
    			long timestamp = getFields(bytebuffer);
    			
    			//为不存在的id值创建相应的TreeMap
    			if(nextUpdate != -1 && !firstScan.containsKey(nextUpdate))
    				firstScan.put(nextUpdate, new TreeMap<Long,String>(new Comparator<Long>(){
						@Override
						public int compare(Long o1, Long o2) {
							return o2.compareTo(o1);
						}	
					}));
    			else if(nextUpdate == -1 && !firstScan.containsKey(preUpdate))
    				firstScan.put(preUpdate, new TreeMap<Long,String>(new Comparator<Long>(){
						@Override
						public int compare(Long o1, Long o2) {
							return o2.compareTo(o1);
						}	
					}));
    			
    			//根据类型判断插入hashmap的操作
	    		switch(logType){
	    			case 'I': 
	    					firstScan.get(nextUpdate).put(timestamp, new StringBuilder()
	    							.append(logType).append(":").append(preUpdate)
	    							.append(",").append(nextUpdate).append(",")
	    							.append(fileNo).append(",").append(currentPos)
	    							.append(",").append(len).toString());
	    					break;
	    			case 'U':
	    					firstScan.get(nextUpdate).put(timestamp,new StringBuilder()
	    							.append(logType).append(":").append(preUpdate).append(",")
	    							.append(nextUpdate).append(",").append(fileNo).append(",").append(currentPos)
	    							.append(",").append(len).append(",").append(updateFields).toString());
	    					break;
	    			case 'D':
	    					firstScan.get(preUpdate).put(timestamp,new StringBuilder()
								.append(logType).append(":").append(preUpdate)
								.append(",").append(nextUpdate).append(",")
								.append(fileNo).append(",").append(currentPos)
								.append(",").append(len).toString());
    				}
    			}
    			
//    			LogMessage logMessage = new LogMessage(currentPos, logType, preUpdate, nextUpdate, timestamp); 
//    			firstScan.put(timestamp, logMessage);
//    			findNext(bytebuffer);
//    			findNext(bytebuffer);
//    			findNext(bytebuffer);
//    			
//    			byte[] idRange= new byte[64];
//    			int idLen = -1;
//    			while((readBuffer = bytebuffer.get()) != '|'){
//    				idRange[++idLen] = readBuffer;
//    			}
//    			long id = parseLongFromStrBytes(idRange, idLen + 1);
//    			if(id >= beginId && id <= endId)
//    				System.out.println("匹配成功！");
    		}
    	}

    private String getUpdateFields(ByteBuffer bytebuffer){
//    	try {
//    		String str = new String(bytebuffer.array(), bytebuffer.position(), bytebuffer.remaining(), "utf8");
//    		System.out.println(str);
//    	} catch (UnsupportedEncodingException e) {
//    		
//    	}
    	byte[] byteFields = new byte[1024];
    	byte offset;
    	int len = -1;
    	while(bytebuffer.remaining() > 1){  //之所以大于1是因为最后有个“\n”
    		while((offset = bytebuffer.get()) != (byte)':'){
        		byteFields[++len] = offset;
        	} 		
    		byteFields[++len] = ',';
    		while(bytebuffer.get() != '|'){
        		
        	}
			findNext(bytebuffer);
			findNext(bytebuffer);
		}
    	String value = new String(byteFields,0,len + 1);
    	return value;
    }
    private long getFields(ByteBuffer bytebuffer) {
		byte offset;
		if((offset = bytebuffer.get()) == (byte)'N'){
			findNext(bytebuffer);
			return 0;
		}else{
			byte[] byteFields = new byte[64];
			byteFields[0] = offset;
			int i = 0;
			while((offset = bytebuffer.get()) != '|'){
				byteFields[++i] = offset;
			}
 			long value = bytesToLong(byteFields, 0,i + 1);
			return value;
		}
	}
    public static long bytesToLong(byte[] bytes, int offset, int limit) {
		   char c;              /* current char */
	        long total;         /* current total */
	        int sign;           /* if '-', then negative, otherwise positive */

	        c = (char)bytes[offset];
	        sign = c;           /* save sign indication */
	        if (c == '-' || c == '+')
	            offset++;    /* skip sign */

	        total = 0;
	        while (offset < limit) {
	            c = (char)bytes[offset];    /* get next char */
	            total = 10 * total + (c - '0');     /* accumulate digit */
	            offset++;
	        }

	        if (sign == '-')
	            return -total;
	        else
	            return total;

    }
    private byte compareByte(ByteBuffer bytebuffer, byte[] compareByte, int len){
    	int i = 0;
    	while(i < len && bytebuffer.get() == compareByte[i]){
    		i++;
    	}
    	if(i == len)
    		return bytebuffer.get();
    	else
    		return -1;
    }
    private void findNext(ByteBuffer bytebuffer){
    	while(bytebuffer.get() != '|'){
    	}
    }
    
    public static class LogMessage{
    	private final long position;
    	private final byte type;
    	private final long preUpdate;
    	private final long nextUpdate;
    	private final long timestamp;
		public long getTimestamp() {
			return timestamp;
		}
		public LogMessage(long position, byte type, long preUpdate, long nextUpdate,long timestamp) {
			super();
			this.position = position;
			this.type = type;
			this.preUpdate = preUpdate;
			this.nextUpdate = nextUpdate;
			this.timestamp = timestamp;
		}
		public long getPosition() {
			return position;
		}
		public byte getType() {
			return type;
		}
		public long getPreUpdate() {
			return preUpdate;
		}
		public long getNextUpdate() {
			return nextUpdate;
		}
    }
    public static class Block {
    	private final long position;
    	private final ByteBuffer buffer;
    	public Block(ByteBuffer buf, long pos) {
    		position = pos;
    		buffer = buf;
    	}
		public long getPosition() {
			return position;
		}
		public ByteBuffer getBuffer() {
			return buffer;
		}
    	
    }
}
