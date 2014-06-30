package com.anand.hadoop.pattern.join.Simple;

import com.anand.hadoop.pattern.util.Item;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by analog76 on 6/28/14.
 */
public class SimpleJoinItem extends Mapper<LongWritable,Text,IntWritable,Text> {

    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

        Item it = new Item();
        it.parse(value.toString());
        Text key1 = new Text();
        Text value1 = new Text();

        IntWritable it1  = new IntWritable();
        it1.set(it.getMovieId());
        key1.set(it.getMovieId()+"");


        StringBuffer sb= new StringBuffer();
        sb.append("item,");
        sb.append(it.getTitle()+"  ,");
        sb.append(it.getReleaseDate()+"  ,");


        value1.set(sb.toString());
        context.write(it1, value1);
    }
}
