package com.anand.hadoop.pattern.filter.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by analog76 on 6/23/14.
 */
public class KeyPartitioner extends Partitioner<CustomKey,Text> {


    @Override
    public int getPartition(CustomKey customKey, Text text, int numPartitioner) {

        int hash = customKey.symbol.hashCode();
        int partition=hash%numPartitioner;
        return partition;
    }
}
