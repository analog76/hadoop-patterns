package com.anand.hadoop.pattern.simple;

import com.anand.hadoop.pattern.simple.combined.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by analog76 on 4/26/14.
 */
public class SimpleMapReduce {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        String strInput="/home/analog76/Downloads/ml-100k/u*";
    //    strInput="/home/analog76/Documents/stackexchange";

        String strOutput="/home/analog76/Downloads/MapReduceOutput/Simple/";
        Configuration conf = new Configuration();
        Job job = new Job(conf);

        //set input and output path.
        Path  inputPath = new Path(strInput);
        Path outputPath  = new Path(strOutput);


        if(outputPath.getFileSystem(conf).exists(outputPath)){

            outputPath.getFileSystem(conf).delete(outputPath,true);
        }

        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);


        //set input and output format class.
        job.setInputFormatClass(CombinedInputFormat.class);
   //     job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        //Setmapper and reducer class
        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);

        //set output key/value class.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);
    }



}
