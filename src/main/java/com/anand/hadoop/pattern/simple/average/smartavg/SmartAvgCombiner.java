package com.anand.hadoop.pattern.simple.average.smartavg;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by analog76 on 5/31/14.
 */
public class SmartAvgCombiner extends Reducer<Text, SortedMapWritable, Text, SortedMapWritable> {

    SortedMapWritable outvalues = new SortedMapWritable();

    public void reduce(Text key,Iterable<SortedMapWritable> values ,Context context){


    }
}
