package com.anand.hadoop.pattern.simple.average;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by analog76 on 5/10/14.
 */
public class AvgReducer extends Reducer<Text,DoubleWritable,Text,Text>{


    public void reducer(Text key, Iterable<AvgUtilClass> values,Context ctx) throws IOException, InterruptedException {

        double sum=0.0d;
        int cnt=0;
        for(AvgUtilClass val:values){
            sum = sum + val.getSum();
            cnt = cnt + val.getCount();
        }
        double avg  = sum/cnt;

        ctx.write(key,new Text(avg+" "+cnt));
        /*
        double sum=0.0d;
        int cnt=0;
        for(DoubleWritable val:values){
            sum = sum+val.get();
            cnt = cnt + 1;
        }
        double avg  = sum/cnt;

        ctx.write(key,new Text(avg+" "+cnt));
        */
    }
}
