package com.anand.hadoop.pattern.filter.totalsortorder;

import com.anand.hadoop.pattern.util.ParseLine;
import com.anand.hadoop.pattern.util.Stock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by analog76 on 6/30/14.
 */
public class TotalSortMapper extends Mapper<LongWritable, Text,Text,Text> {
    ParseLine parser = new ParseLine();

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        Stock s = parser.parse(value.toString());

         context.write(new Text(s.getSymbol()),value);

    }
}
