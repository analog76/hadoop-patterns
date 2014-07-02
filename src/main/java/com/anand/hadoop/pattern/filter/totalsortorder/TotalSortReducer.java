package com.anand.hadoop.pattern.filter.totalsortorder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.awt.*;
import java.io.IOException;

/**
 * Created by analog76 on 6/30/14.
 */
public class TotalSortReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
           for(Text value: values){
               context.write(key,value);
           }
    }
}
