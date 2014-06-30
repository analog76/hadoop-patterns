package com.anand.hadoop.pattern.simple.average;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by analog76 on 5/10/14.
 */
public class AvgReducer1  extends Reducer<Text,AvgUtilClass,Text,AvgUtilClass> {


    @Override
    public void reduce(Text key, Iterable<AvgUtilClass> values,Context ctx) throws IOException, InterruptedException {


        double min=Double.MAX_VALUE, max=Double.MIN_VALUE;
        AvgUtilClass util = new AvgUtilClass();
        double sum=0.0d;
        int cnt=0;
        for(AvgUtilClass val:values){
            ///System.out.println("sum "+val.toString());
            sum = sum + val.getAvg();
            if( min >val.getAvg()){
                    min=val.getAvg();
            }
            if(max< val.getAvg()){
                max=val.getAvg();
            }
            cnt = cnt + val.getCount();
        }
        double avg  = sum/cnt;

        System.out.println(Math.round(sum*100)/100.0d);

        util.setAvg(Math.round(avg*100)/100.0d);
        util.setCount(cnt);
        util.setSum(Math.round(sum*100)/100.0d);
        util.setMax(Math.round(max*100)/100.0d);
        util.setMin(Math.round(min*100)/100.0d);

        ctx.write(key,util);

    }

}
