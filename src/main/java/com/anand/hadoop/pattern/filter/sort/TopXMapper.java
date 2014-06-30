package com.anand.hadoop.pattern.filter.sort;

import com.anand.hadoop.pattern.util.ParseLine;
import com.anand.hadoop.pattern.util.Stock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by analog76 on 6/28/14.
 */
public class TopXMapper extends Mapper <LongWritable,Text,CustomKey,CustomKey> {
    ParseLine parse= new ParseLine();
    public void map(LongWritable key, Text value,Mapper.Context ctx) throws IOException, InterruptedException {

        Stock s = parse.parse(value.toString());

        CustomKey customKey = new CustomKey();
        customKey.setPrice(s.getAdjPrice());
        customKey.setSymbol(s.getSymbol());
        ctx.write(customKey,customKey);

    }
}
