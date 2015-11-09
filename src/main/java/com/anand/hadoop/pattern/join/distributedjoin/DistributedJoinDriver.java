package com.anand.hadoop.pattern.join.distributedjoin;

import com.anand.hadoop.pattern.join.Simple.SimpleJoinItem;
import com.anand.hadoop.pattern.join.Simple.SimpleJoinRating;
import com.anand.hadoop.pattern.join.Simple.SimpleJoinReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by analog76 on 7/1/14.
 */
public class DistributedJoinDriver {

    public  static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String strUser  = "/Users/anand.ranganathan/github/ml-100k/u.user";
        String strItem  = "/Users/anand.ranganathan/github/ml-100k/u.item";
        String strItemLocal  = "/tmp/ml-100k/u.item";

        String strRating  = "/Users/anand.ranganathan/github/ml-100k/*.base";

        String strOutput="/Users/anand.ranganathan/github/MapReduceOutput/distributedjoin/";

        Path outputPath = new Path(strOutput);


        Configuration conf = new Configuration();
        Job job = new Job(conf);



        if(outputPath.getFileSystem(conf).exists(outputPath)){

            outputPath.getFileSystem(conf).delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job, strRating);
        FileOutputFormat.setOutputPath(job, outputPath);



        job.setMapperClass(DistributedJoinMapper.class);

        DistributedCache.addCacheFile(new Path(strItem).toUri(),job.getConfiguration());
      //  DistributedCache.setLocalFiles(job.getConfiguration(),strItemLocal);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        boolean x = job.waitForCompletion(true);
        if(x)
            System.out.println(" execution compmleted ");


    }
}
