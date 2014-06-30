package com.anand.hadoop.pattern.filter.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by analog76 on 6/26/14.
 */
public class CompositeKeyPartitioner extends Partitioner<CompositeKey, Text>{


        @Override
    public int getPartition(CompositeKey compositeKey, Text text, int i) {

            int hash = compositeKey.hashCode();
            int n = hash % i;
            return n;
    }

}
