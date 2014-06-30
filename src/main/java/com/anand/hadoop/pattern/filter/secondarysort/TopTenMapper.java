package com.anand.hadoop.pattern.filter.secondarysort;

import com.anand.hadoop.pattern.util.ParseLine;
import com.anand.hadoop.pattern.util.Stock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by analog76 on 6/23/14.
 */
public class TopTenMapper extends Mapper<LongWritable,Text,CustomKey,Text> {

    ParseLine parser = new ParseLine();
    CompositeKey ck;
    Text t = new Text();
    public void map(LongWritable key , Text value,Context ctx) throws IOException, InterruptedException {

        Stock s = parser.parse(value.toString());
        CustomKey ck = new CustomKey();
        if(!s.getSymbol().equalsIgnoreCase("stock_symbol")){
            ck.setSymbol(s.getSymbol());
            ck.setPrice(s.getAdjPrice());
            t.set(s.getSymbol()+ " "+s.getAdjPrice());
   //         System.out.println(ck.toString() + ck.hashCode());
            ctx.write(ck,t);
        }

     }
}
