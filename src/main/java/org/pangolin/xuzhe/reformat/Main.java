package org.pangolin.xuzhe.reformat;

import java.io.*;
import java.util.zip.InflaterInputStream;

public class Main {

    public static void main(String[] args) throws Exception{
        FileInputStream input = new FileInputStream("middle.dat");
        DataInputStream dataInputStream = new DataInputStream(input);
        int blockSize = dataInputStream.readInt();
        byte[] rawData = new byte[blockSize];
        dataInputStream.read(rawData);
        byte[] data = uncompress(rawData, rawData.length);
        dataInputStream = new DataInputStream(new ByteArrayInputStream(data));
        while(true) {
            byte op = dataInputStream.readByte();
            if (op == 'I') {
                System.out.println("I");
            }
            long oldPK = dataInputStream.readLong();
            System.out.printf("oldPK:%d\n", oldPK);
            System.out.printf("newPK:%d\n", dataInputStream.readLong());
            int columnCount = dataInputStream.readByte();
            System.out.printf("columnCount:%d\n", columnCount);
            for (int i = 0; i < columnCount; i++) {
                int columnNo = dataInputStream.readByte();
                int valueLen = dataInputStream.readByte();
                byte[] value = new byte[valueLen];
                dataInputStream.read(value);
                System.out.printf("%d\t%d\t%s\n", columnNo, valueLen, new String(value));
            }
            if(System.in.read() == '1') break;

        }
        dataInputStream.close();
        input.close();
    }

    public static byte[] uncompress(final byte[] src, int blockSize) throws IOException {
        byte[] result = src;
        byte[] uncompressData = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src, 0, blockSize);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

        try {
            while (true) {
                int len = inflaterInputStream.read(uncompressData, 0, uncompressData.length);
                if (len <= 0) {
                    break;
                }
                byteArrayOutputStream.write(uncompressData, 0, len);
            }
            byteArrayOutputStream.flush();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException ignored) {
            }
            try {
                inflaterInputStream.close();
            } catch (IOException ignored) {
            }
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignored) {
            }
        }

        return result;
    }
}
