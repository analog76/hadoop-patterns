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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by analog76 on 6/30/14.
 */
public class TotalSortDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String strInput="/Users/anand.ranganathan/github/nasdaq-outliers/data/NASDAQ/NASDAQ_daily*B.csv";
        //    strInput="/home/analog76/Documents/stackexchange";

        strInput="/Users/anand.ranganathan/github/ml-100k/*.base";

        String strOutput="/Users/anand.ranganathan/github//MapReduceOutput/totalsort";

        Configuration conf = new Configuration();
        Job job = new Job(conf);

        //set input and output path.
        Path inputPath = new Path(strInput);
        Path outputPath  = new Path(strOutput);

        Path partitionFile = new Path(strOutput + "_partitions.lst");
        Path outputStage = new Path(strOutput + "_staging");

        // Output Path

        double sampleRate = Double.parseDouble("1");


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

        TextInputFormat.setInputPaths(job, inputPath);

        // Set the output format to a sequence file

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
   //     job.setOutputFormatClass(FileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job, outputStage);

        // Submit the job and get completion code.

        int code;

        code = job.waitForCompletion(true)?0:1 ;
         if(code==0){
            Job totalSortJob =new Job();
            totalSortJob.setMapperClass(Mapper.class);
       //     totalSortJob.setReducerClass(TotalSortReducer.class);
       //     totalSortJob.setNumReduceTasks(10);
            totalSortJob.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(totalSortJob.getConfiguration(), partitionFile);

            totalSortJob.setOutputKeyClass(Text.class);
            totalSortJob.setOutputValueClass(Text.class);

            totalSortJob.setInputFormatClass(SequenceFileInputFormat.class);
            FileInputFormat.setInputPaths(totalSortJob, outputStage);

            FileOutputFormat.setOutputPath(totalSortJob, outputPath);
      //      totalSortJob.getConfiguration().set(
      //              "mapred.textoutputformat.separator", "");

     //        InputSampler.writePartitionFile(totalSortJob,
     //                new InputSampler.RandomSampler(1, 1000));


             InputSampler.writePartitionFile(totalSortJob,
                    new InputSampler.RandomSampler(sampleRate, 100000));

              code  = totalSortJob.waitForCompletion(true)? 0 : 2;


            //if(code)
            {
                System.out.println(" total sort completed");
            }

        }
            System.out.println(" execution compmleted ");


    }
}
