package com.anand.hadoop.pattern.filter.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by analog76 on 6/23/14
 */
public class TopTenReducer extends Reducer<CustomKey,Text,CustomKey,Text> {


    public void reduce(CustomKey key,Iterable<Text> values,Context ctx) throws IOException, InterruptedException {

        Text t = new Text();
        t.set(key.toString()+" "+key.price);

        System.out.println(key.symbol);

         ctx.write(key, t);

    }

}
