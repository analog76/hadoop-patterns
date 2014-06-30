package com.anand.hadoop.pattern.simple.average.smartavg;

import com.anand.hadoop.pattern.simple.average.AvgUtilClass;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by analog76 on 5/31/14.
 */
public class SmartAvgReducer extends Reducer<Text,SortedMapWritable,Text,Text> {

    private TreeMap<Double, Long> commentLengthCounts =
            new TreeMap<Double, Long>();

    public void reduce(Text key,Iterable<SortedMapWritable> values,Context context) throws IOException, InterruptedException {
        double sum = 0.0d;
        long cnt= 0;
        double median=0.0d;
        Text result = new Text();
        commentLengthCounts.clear();

        for(SortedMapWritable value:values){
            for(Map.Entry<WritableComparable,Writable> entry:value.entrySet()){
                AvgUtilClass val = (AvgUtilClass)entry.getValue();
//                int length = ((IntWritable) entry.getKey()).get();
 //               long count = ((LongWritable) entry.getValue()).get();


                sum = sum + val.getAvg();
                cnt = cnt + val.getCount();
         //       System.out.println(sum+" "+cnt);

                Long storedCount = commentLengthCounts.get(val.getAvg());

                if (storedCount == null) {
                    commentLengthCounts.put(val.getAvg(), cnt);
                } else {
                    commentLengthCounts.put(val.getAvg() ,storedCount+ cnt);
                }
            }

        }

        AvgUtilClass util = new AvgUtilClass();

        long medianIndex = cnt / 2L;
        long soforLength=0;

        System.out.println(commentLengthCounts.size()+" "+cnt);

        for (Map.Entry<Double, Long> entry : commentLengthCounts.entrySet()) {
            if(medianIndex >soforLength)
                    soforLength=soforLength+entry.getValue();
            else{
                    util.setMedian(entry.getKey());
            }
        }



    //    System.out.println(Math.round(sum*100)/100.0d);
/*//
        util.setAvg(Math.round(avg*100)/100.0d);
        util.setCount(cnt);
        util.setSum(Math.round(sum*100)/100.0d);
        util.setMax(Math.round(max*100)/100.0d);
        util.setMin(Math.round(min*100)/100.0d);
*/


        double avg  = sum/cnt;

        System.out.println( key.toString()+" "+sum+" "+cnt+" "+avg+" " +util.getMedian());

        context.write(key,new Text(avg+" "+cnt+" "+util.getMedian()));

     }
}
