package com.anand.hadoop.pattern.join.distributedjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by analog76 on 7/2/14.
 */
public class MultipleDistributedJoinReducer extends Reducer<Text,Text,Text,Text> {

    long count=0;

    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
        for(Text value:values){
            context.write(key,value);
            count=count+1;
        }

        System.out.println(key.toString()+" count is  "+count);
    }
}
