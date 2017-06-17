package org.pangolin.xuzhe.positiveorder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ubuntu on 17-6-17.
 */
public class Schema {
    int[] coloumHashCode;
    byte[] columnDataType;
    Map<Integer, Integer> columnHash2NoMap;
    public final int columnCount;
    private Schema(int columnCount) {
        this.columnCount = columnCount;
        coloumHashCode = new int[columnCount];
        columnDataType = new byte[columnCount];
        columnHash2NoMap = new HashMap<>();
    }

    public static Schema generateFromInsertLog(byte[] data) {
        int itemIndex = 0;
        for(int i = 0; i < data.length; ++i) {
            byte b = data[i];
            if (b == '|') {
                itemIndex++;
            }
        }
        int columnCount = itemIndex - 9;
        columnCount = columnCount / 3;
        Schema s = new Schema(columnCount);
        itemIndex = 0;
        int columnIndex = 0;
        for(int i = 0; i < data.length; ++i) {
            byte b = data[i];
            if (b == '|') {
                itemIndex++;
                ++i;
                if(itemIndex == 9) {
//                    System.out.println(new String(data, i, data.length-i-1));
                    // 解析每一列
                    for(int j = 0; j < columnCount; j++) {
                        int hash = 0;
                        while ((b = data[i]) != ':') {
                            hash = 31 * hash + b;
//						    System.out.print((char)b);
                            ++i;
                        }
                        byte dataType = (byte)(data[i+1] - '0');
                        s.columnDataType[columnIndex] = dataType;
                        s.coloumHashCode[columnIndex] = hash;
                        s.columnHash2NoMap.put(hash, columnIndex);
                        ++columnIndex;
//                        System.out.println();
                        // 跳过2个'|'
                        for(int k = 0; k < 3; k++) {
                            while((b = data[i]) != '|') {
                                ++i;
//                                System.out.print((char)b);
                            }
                            ++i;
//                            System.out.print('|');
                        }
//                        System.out.println();
                    }
                }
            }
        }
        System.out.println(columnCount);
        return s;

    }

    public static void main(String[] args) {
        String s = "|mysql-bin.00001717148771|1496736165000|middleware3|student|I|id:1:1|NULL|13|" +
                "first_name:2:0|NULL|孙|last_name:2:0|NULL|益|sex:2:0|NULL|男|score:1:0|NULL|84|";
        byte[] data = s.getBytes();
        Schema schema = generateFromInsertLog(data);
        System.out.println(Arrays.toString(schema.coloumHashCode));
        System.out.println(Arrays.toString(schema.columnDataType));
        System.out.println(schema.columnHash2NoMap);
    }
}