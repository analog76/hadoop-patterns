package com.anand.hadoop.pattern.join.Simple;

import com.anand.hadoop.pattern.util.Rating;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by analog76 on 6/29/14.
 */
public class SimpleJoinRating extends Mapper<LongWritable,Text,IntWritable,Text> {

    Rating r = new Rating();
    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
            r = new Rating();
            r.parse(value.toString());

        Text  key1 = new Text();
        key1.set(r.getMovieId()+"");

        Text value1= new Text();

        IntWritable it1 = new IntWritable();
        it1.set(r.getMovieId());
        value1.set("rating,"+ r.getRating());

        context.write(it1, value1);


    }
}
