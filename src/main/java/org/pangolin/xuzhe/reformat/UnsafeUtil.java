package org.pangolin.xuzhe.reformat;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Created by XuZhe on 2017/6/24.
 */
public class UnsafeUtil {
    public static final Unsafe UNSAFE;
    private static final Field ADDRESS_ACCESSOR;
    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            Field field = Buffer.class.getDeclaredField("address");
            field.setAccessible(true);
            ADDRESS_ACCESSOR = field;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static long getAddress(Buffer buffer)
    {
        try {
            return (long) ADDRESS_ACCESSOR.get(buffer);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) {
        try {
            Field ADDRESS_ACCESSOR = Buffer.class.getDeclaredField("address");
            ADDRESS_ACCESSOR.setAccessible(true);
            DirectBuffer buffer = (DirectBuffer) ByteBuffer.allocateDirect(10);
            long addr = buffer.address();
            long addr2 = (long)ADDRESS_ACCESSOR.get(buffer);
            System.out.println(addr);
            System.out.println(addr2);
            ByteBuffer buf = (ByteBuffer)buffer;
//            buf.put((byte)99);
            long t1 = System.nanoTime();
            for(int i = 0; i < 10000_0000; i++) {
                long addr3 = addr + i % 6;
                UNSAFE.putLong(addr3, 999);
                long a = UNSAFE.getLong(addr3);
            }
            long t2 = System.nanoTime();
            long a = UNSAFE.getLong(addr);
            System.out.println(a + "|" + (t2-t1));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
