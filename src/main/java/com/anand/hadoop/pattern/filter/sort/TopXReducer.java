package com.anand.hadoop.pattern.filter.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by analog76 on 6/28/14.
 */
public class TopXReducer extends Reducer<CustomKey,CustomKey,Text,Text> {

    public void reduce(CustomKey key, Iterable<CustomKey> values,Context context) throws IOException, InterruptedException {

        int topX = Integer.parseInt(  context.getConfiguration().get("top_numbers"));


        System.out.println(" entering the reduce method ");
        int x=10;
        int i=0;

        Text t = new Text();
        t.set(key.getSymbol());

        for(CustomKey value: values) {

                context.write(t, new Text(value.getPrice()+" "));
                i++;
                if(topX<i)
                       break;

                System.out.println("I " + i);
                System.out.println(key.getSymbol() + " " + key.getPrice());


        }

    }
}
