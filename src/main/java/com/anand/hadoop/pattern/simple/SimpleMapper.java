package com.anand.hadoop.pattern.simple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by analog76 on 4/26/14.
 */
public class SimpleMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

     @Override
     public void map(LongWritable key,Text value,Context ctx)throws IOException, InterruptedException{
        String line = value.toString();
         StringTokenizer stringTokenizer = new StringTokenizer(line);
          LongWritable ONE = new LongWritable(1);
          while(stringTokenizer.hasMoreElements()) {
             String word = stringTokenizer.nextToken();
             ctx.write(new Text(word), ONE);
         }
     }
}
