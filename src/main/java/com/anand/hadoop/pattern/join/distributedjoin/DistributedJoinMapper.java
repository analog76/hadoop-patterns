package com.anand.hadoop.pattern.join.distributedjoin;

import com.anand.hadoop.pattern.util.Item;
import com.anand.hadoop.pattern.util.ParseLine;
import com.anand.hadoop.pattern.util.Rating;
import com.anand.hadoop.pattern.util.Stock;
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
public class DistributedJoinMapper extends Mapper<LongWritable,Text,Text,Text> {


    // Load Movies and Rating from different files.
    // Display the output based on movie_id,Movie_name,Rating.

    Map<Integer,String> m = new HashMap<Integer,String>();
    ParseLine parser  = new ParseLine();
    Stock s = new Stock();
    Item it = new Item();
    Rating rating = new Rating();

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

        rating = new Rating();
        rating.parse(value.toString());

        String s  = m.get(rating.getMovieId());
        Text value1 = new Text();
        value1.set(s+" , "+ rating.getRating());
        context.write(new Text(rating.getMovieId()+" "),value1);

    }


    public void setup(Context context) throws IOException {
        Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(files[0].toString())));

          String line=null;

            while ((line = bf.readLine()) != null) {

                if(line!=null){
                    it = new Item();
                    it.parse(line);
                    m.put(it.getMovieId(),it.getTitle()+" ,"+ it.getReleaseDate());
                }
            }

    }
}
