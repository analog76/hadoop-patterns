package com.anand.hadoop.pattern.filter.partition;

import com.anand.hadoop.pattern.util.ParseLine;
import com.anand.hadoop.pattern.util.Stock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by analog76 on 6/29/14.
 */
public class PartitionMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

        ParseLine parser = new ParseLine();
        Stock s = parser.parse(value.toString());
     //   System.out.println(s.getSymbol());
        context.write(new Text(s.getSymbol()),new Text(s.toString()));

    }
}
