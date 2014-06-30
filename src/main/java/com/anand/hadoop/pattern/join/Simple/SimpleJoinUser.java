package com.anand.hadoop.pattern.join.Simple;

import com.anand.hadoop.pattern.util.User;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by analog76 on 6/28/14.
 */
public class SimpleJoinUser extends Mapper<LongWritable,Text,Text,Text> {

    User user = new User();
    Text key1 = new Text();
    Text value1 = new Text();

    public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {

        user  = new User(value.toString());
        key1 = new Text();
//     /   key1.set(user.getUserId()+"");
        //  key = new Text();

  //       value = new Text();
         context.write(key1,key1);


    }


}
