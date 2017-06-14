package org.pangolin.xuzhe.pipeline;

/**
 * Created by 29146 on 2017/6/14.
 */
public class HeapSort {
    public static void heapSorting(Record[] a, int len){

        buildHeap(a,len);    //建堆能够保证是小顶堆，自下而上
        for(int i=len-1;i>0;i--){
            swap(a,0,i);
            exchange(a,i,0);
        }

    }
    private static void buildHeap(Record[] a,int len){
        for(int i=len/2-1;i>=0;i--){
            exchange(a,len,i);
        }
    }
    private static void exchange(Record[] a,int len,int pos){
        int left,right,mix;
        left=pos*2+1;
        right=pos*2+2;
        mix=pos;
        if(left<len&&a[left].getPk()>a[mix].getPk())  //重点就在left小于b上
            mix=left;
        if(right<len&&a[right].getPk()>a[mix].getPk())
            mix=right;
        if(mix!=pos){
            swap(a,mix,pos);
            exchange(a, len, mix);
        }
    }
    private static void swap(Record[] a,int q,int w){
        Record c;
        c=a[q];
        a[q]=a[w];
        a[w]=c;
    }
}
