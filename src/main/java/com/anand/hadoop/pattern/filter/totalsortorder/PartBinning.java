package com.anand.hadoop.pattern.filter.totalsortorder;

import com.anand.hadoop.pattern.util.ParseLine;
import com.anand.hadoop.pattern.util.Stock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by anand.ranganathan on 11/8/15.
 */
public class PartBinning extends Mapper<LongWritable,Text,Text,Text> {

    MultipleOutputs<Text,Text> multipleOutputs = null;
    ParseLine parser = new ParseLine();
    public void setup(Mapper.Context context){
        this.multipleOutputs=new MultipleOutputs<Text, Text>(context);

    }


    public void map(LongWritable key,Text value,Mapper.Context context) throws IOException, InterruptedException {
        Stock s = parser.parse(value.toString());
        multipleOutputs.write("bins",value, NullWritable.get(),s.getSymbol());

    }

    public void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
