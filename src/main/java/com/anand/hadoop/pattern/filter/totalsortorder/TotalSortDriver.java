package com.anand.hadoop.pattern.filter.totalsortorder;

import com.anand.hadoop.pattern.filter.distinct.DistinctMapper;
import com.anand.hadoop.pattern.filter.distinct.DistinctReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.InputSampler;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by analog76 on 6/30/14.
 */
public class TotalSortDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String strInput="/home/analog76/Downloads/infochimps_dataset_4777_download_16185/NASDAQ/NASDAQ_daily*B.csv";
        //    strInput="/home/analog76/Documents/stackexchange";

        String strOutput="/home/analog76/Downloads/MapReduceOutput/totalsort";

        Configuration conf = new Configuration();
        Job job = new Job(conf);

        //set input and output path.
        Path inputPath = new Path(strInput);
        Path outputPath  = new Path(strOutput);

        Path partitionFile = new Path(strOutput + "_partitions.lst");
        Path outputStage = new Path(strOutput + "_staging");

        // Output Path

        double sampleRate = Double.parseDouble("100.0");


        FileSystem.get(new Configuration()).delete(outputPath, true);
        FileSystem.get(new Configuration()).delete(outputStage, true);
        FileSystem.get(new Configuration()).delete(partitionFile, true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputStage);


        //set input and output format class.
         job.setInputFormatClass(TextInputFormat.class);


        //Setmapper and reducer class

        job.setMapperClass(TotalSortMapper.class);

        //set output key/value class.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        boolean x = job.waitForCompletion(true);
        if(x){
            Job totalSortJob =new Job();
            totalSortJob.setMapperClass(Mapper.class);
            totalSortJob.setReducerClass(TotalSortReducer.class);
            totalSortJob.setNumReduceTasks(10);
            totalSortJob.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(totalSortJob.getConfiguration(), partitionFile);
            totalSortJob.setOutputKeyClass(Text.class);
            totalSortJob.setOutputValueClass(Text.class);
            totalSortJob.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(totalSortJob, outputStage);
            FileOutputFormat.setOutputPath(totalSortJob, outputPath);
            totalSortJob.getConfiguration().set(
                    "mapred.textoutputformat.separator", "");

            InputSampler.writePartitionFile(totalSortJob,
                    new InputSampler.RandomSampler(sampleRate, 10000));

            x = totalSortJob.waitForCompletion(true);


            if(x){
                System.out.println(" total sort completed");
            }

        }
            System.out.println(" execution compmleted ");


    }
}
