package com.anand.hadoop.pattern.filter.distinct;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by analog76 on 6/23/14.
 */
public class DistinctReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {


            ctx.write(key,key);
    }
}
