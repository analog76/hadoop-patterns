package com.anand.hadoop.pattern.filter.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by analog76 on 6/29/14.
 */
public class PartitionDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        String strInput="/home/analog76/Downloads/infochimps_dataset_4777_download_16185/NASDAQ/NASDAQ_daily*B.csv";
        //    strInput="/home/analog76/Documents/stackexchange";

        String strOutput="/home/analog76/Downloads/MapReduceOutput/partition/";
        Configuration conf = new Configuration();
        Job job = new Job(conf);

        //set input and output path.
        Path inputPath = new Path(strInput);
        Path outputPath  = new Path(strOutput);


        if(outputPath.getFileSystem(conf).exists(outputPath)){

            outputPath.getFileSystem(conf).delete(outputPath,true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);


        //set input and output format class.
        //   job.setInputFormatClass(CombinedInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        //   job.setOutputFormatClass(TextOutputFormat.class);

        //Setmapper and reducer class

        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);
        job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(4);

        //set output key/value class.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        boolean x = job.waitForCompletion(true);
        if(x)
            System.out.println(" execution compmleted ");
    }
}
