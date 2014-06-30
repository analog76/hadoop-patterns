package com.anand.hadoop.pattern.filter.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by analog76 on 6/28/14.
 */
public class TopXDriver {




    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        String strInput="/home/analog76/Downloads/infochimps_dataset_4777_download_16185/NASDAQ/NASDAQ_daily*C.csv";
        //    strInput="/home/analog76/Documents/stackexchange";

        String strOutput="/home/analog76/Downloads/MapReduceOutput/topten/";
        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.getConfiguration().set("top_numbers","20");


        //set input and output path.
        Path inputPath = new Path(strInput);
        Path outputPath  = new Path(strOutput);

        System.out.println(" execution compmleted ");

        if(outputPath.getFileSystem(conf).exists(outputPath)){

            outputPath.getFileSystem(conf).delete(outputPath,true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);


        job.setMapperClass(TopXMapper.class);
        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(CustomKey.class);


        job.setReducerClass(TopXReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setSortComparatorClass(SortComparator.class);



        boolean x = job.waitForCompletion(true);
        if(x)
            System.out.println(" execution compmleted ");
    }

}
