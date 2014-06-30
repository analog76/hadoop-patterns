package com.anand.hadoop.pattern.filter.distinct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by analog76 on 6/21/14.
 */
public class SimpleFilter extends Mapper<LongWritable,Text,LongWritable ,Text  >{

        public void map(LongWritable key,Text value, Context ctx){

       //     ctx.write()


        }

}
