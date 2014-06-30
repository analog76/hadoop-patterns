package com.anand.hadoop.pattern.filter.distinct;

import com.anand.hadoop.pattern.util.ParseLine;
import com.anand.hadoop.pattern.util.Stock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by analog76 on 6/23/14.
 */
public class DistinctMapper extends Mapper<LongWritable, Text,Text,Text>{

            ParseLine parser = new ParseLine();
            Stock s = new Stock();
            Text t = new Text();

    public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

        s = parser.parse(value.toString());
        t = new Text();
        t.set(s.getSymbol());
        ctx.write(t,  t);
    }
}
