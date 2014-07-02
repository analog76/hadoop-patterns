package com.anand.hadoop.pattern.join.innerouter;

import com.anand.hadoop.pattern.util.Item;
import com.anand.hadoop.pattern.util.Rating;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by analog76 on 6/29/14.
 */
public class InnserOuterJoinReducer extends Reducer<IntWritable,Text,Text,Text> {

    DecimalFormat df = new DecimalFormat("#.##");

    List<String> lstItem = new ArrayList();

    List<String> lstRating = new ArrayList();
    public void reduce(IntWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

        double ratingGreaterThan = Double.parseDouble(ctx.getConfiguration().get("ratingGreaterThan"));


        String joinType = ctx.getConfiguration().get("joinType");
        String movieName=null;
        String movieReleasedDate  = null;
        int ratingCnt=0;
        int ratingSum=0;
        int rating=0;

        lstItem.clear();
        lstRating.clear();

        for(Text value:values){
            String valueRecord=  value.toString();
            String[] str=valueRecord.split(",");
            if(str[0].equalsIgnoreCase("item")){
                movieName=str[1];
                movieReleasedDate=str[2];

             }else{
                ratingCnt++;
                rating= Integer.parseInt(str[1]);
                ratingSum=ratingSum+Integer.parseInt(str[1]);
            }

            if(joinType.equalsIgnoreCase("left") && movieName!=null){
      //          System.out.println(movieName+" , "+ rating+ " ,"+movieReleasedDate);
                ctx.write(new Text(movieName+" "), new Text(rating + ", \t"+movieReleasedDate+" "));
            }
        }


        double avgRating = (double)ratingSum/ratingCnt;
        avgRating = Double.valueOf(df.format(avgRating));





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
