package com.anand.hadoop.pattern.filter.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by analog76 on 6/29/14.
 */
public class CustomPartitioner extends Partitioner<Text,Text> {




    @Override
    public int getPartition(Text text, Text text2, int i) {
        int x =  text.hashCode();
        x=Math.abs(x);
        x=x%i;
        return x;
    }
}
