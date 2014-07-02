package com.anand.hadoop.pattern.join.innerouter;

import com.anand.hadoop.pattern.join.Simple.SimpleJoinItem;
import com.anand.hadoop.pattern.join.Simple.SimpleJoinRating;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by analog76 on 6/28/14.
 */
public class InnerOuterJoinDriver {



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        String strUser  = "/home/analog76/Downloads/ml-100k/u.user";
        String strItem  = "/home/analog76/Downloads/ml-100k/u.item";
        String strRating  = "/home/analog76/Downloads/ml-100k/*.base";

        String strOutput="/home/analog76/Downloads/MapReduceOutput/InnerOuterJoin/";

        Path outputPath = new Path(strOutput);







        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.getConfiguration().set("top_numbers", "20");


        if(outputPath.getFileSystem(conf).exists(outputPath)){

            outputPath.getFileSystem(conf).delete(outputPath,true);
        }

        FileOutputFormat.setOutputPath(job, outputPath);


        //    MultipleInputs.addInputPath(job,new Path(strUser), TextInputFormat.class,SimpleJoinUser.class);
        MultipleInputs.addInputPath(job,new Path(strItem), TextInputFormat.class,SimpleJoinItem.class);
        MultipleInputs.addInputPath(job,new Path(strRating), TextInputFormat.class,SimpleJoinRating.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(InnserOuterJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set("ratingGreaterThan","3.5");

        job.getConfiguration().set("joinType","left");


        boolean x = job.waitForCompletion(true);
        if(x)
            System.out.println(" execution compmleted ");




    }
}
