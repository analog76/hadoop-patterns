package com.anand.hadoop.pattern.simple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by analog76 on 4/26/14.
 */
public class SimpleReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    long count=0;


    public void reduce(Text key,Iterable<LongWritable> values,Context context)
            throws IOException, InterruptedException {

        count=0;
        for(LongWritable value:values) {
            count=count+1;
        }

        LongWritable l = new LongWritable(count);
        context.write(key,l);
    }

}

