package com.anand.hadoop.pattern.filter.sort;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by analog76 on 6/28/14.
 */
public class KeyPartitioner extends Partitioner<CustomKey,CustomKey> {
    @Override
    public int getPartition(CustomKey customKey, CustomKey nullWritable, int i) {
        int hash = customKey.getSymbol().hashCode();
        int partition=hash%i;
        return partition;
    }
}
