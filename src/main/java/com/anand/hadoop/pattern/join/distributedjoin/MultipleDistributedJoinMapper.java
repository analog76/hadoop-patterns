package com.anand.hadoop.pattern.join.distributedjoin;

import com.anand.hadoop.pattern.util.Item;
import com.anand.hadoop.pattern.util.Rating;
import com.anand.hadoop.pattern.util.User;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by analog76 on 7/1/14.
 */
public class MultipleDistributedJoinMapper extends Mapper<LongWritable,Text,Text,Text> {


    Map<Integer, Item> movieMap = new HashMap<Integer, Item>();
    Map<Integer, User> userMap = new HashMap<Integer, User>();


    Item item = new Item();
    User user = new User();
    Rating rating = new Rating();

    Text key1 = new Text();
    Text value1 = new Text();


    public void setup(Context context) throws IOException {
        Path[] files= DistributedCache.getLocalCacheFiles(context.getConfiguration());

        BufferedReader movieReader = new BufferedReader(new InputStreamReader((new FileInputStream(files[0].toString()))));
        BufferedReader userReader = new BufferedReader(new InputStreamReader(new FileInputStream(files[1].toString())));

        String line;

        while((line=movieReader.readLine())!=null){
            item=new Item();
            item.parse(line);
            movieMap.put(item.getMovieId(),item);
        }

        while((line=userReader.readLine())!=null){
            user = new User();
            user.parse(line);
            userMap.put(user.getId(),user);
        }
    }


    User tempUser = null;
    Item tempItem = null;
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

        rating = new Rating();

        rating.parse(value.toString());

     //   System.out.println(value.toString());
        tempItem = movieMap.get(rating.getMovieId());
        tempUser = userMap.get(rating.getUserId());

   //     System.out.println(rating.getMovieId()+", "+tempItem.getMovieId()+", " +rating.getUserId()+","+tempUser.getId()+", "+rating.getRating()+" "+tempItem.getTitle());

          key1.clear();
          value1.clear();
          key1.set(tempItem.getTitle());
          value1.set(tempUser.getJobDesc()+", "+tempUser.getAge()+","+rating.getRating());

        context.write(key1, value1);
    }

}
