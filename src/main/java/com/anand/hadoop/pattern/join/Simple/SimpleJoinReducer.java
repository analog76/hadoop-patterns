package com.anand.hadoop.pattern.join.Simple;

import com.anand.hadoop.pattern.util.Item;
import com.anand.hadoop.pattern.util.Rating;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by analog76 on 6/29/14.
 */
public class SimpleJoinReducer extends Reducer<IntWritable,Text,Text,Text> {

    DecimalFormat df = new DecimalFormat("#.##");
    public void reduce(IntWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

        double ratingGreaterThan = Double.parseDouble(ctx.getConfiguration().get("ratingGreaterThan"));

        String movieName=null;
        String movieReleasedDate  = null;
        int ratingCnt=0;
        int ratingSum=0;
        for(Text value:values){
            String valueRecord=  value.toString();
            String[] str=valueRecord.split(",");
            if(str[0].equalsIgnoreCase("item")){
                movieName=str[1];

                if(key.get()==(267)){
                    System.out.print(movieName+" ");

                }
                System.out.print(movieName+" ");

                movieReleasedDate=str[2];
                System.out.println(movieReleasedDate);
            }else{
                ratingCnt++;
                ratingSum=ratingSum+Integer.parseInt(str[1]);
            }
        }


        double avgRating = (double)ratingSum/ratingCnt;
        avgRating = Double.valueOf(df.format(avgRating));


        if(avgRating>ratingGreaterThan) {
            ctx.write(new Text(movieName), new Text(avgRating + ", \t"+movieReleasedDate));
        }


    }


    public void reduce1(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {

        Text key1 = new Text();

        System.out.println(" inside reducer ");

        Text value1 = new Text();

        for(Object value:values){
            if(value instanceof Item){

            }else if (value instanceof Rating ){

            }
        }



        context.write(key1,value1);
    }
}
