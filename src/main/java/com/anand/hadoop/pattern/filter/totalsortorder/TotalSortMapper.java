package com.anand.hadoop.pattern.filter.totalsortorder;

import com.anand.hadoop.pattern.util.ParseLine;
import com.anand.hadoop.pattern.util.Rating;
import com.anand.hadoop.pattern.util.Stock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by analog76 on 6/30/14.
 */
public class TotalSortMapper extends Mapper<LongWritable, Text,Text,Text> {
    ParseLine parser = new ParseLine();
    Rating rating = new Rating();
    Map<Integer,String> m = new HashMap<Integer,String>();

    Timestamp stamp = new Timestamp(System.currentTimeMillis());
    Date date = new Date(stamp.getTime());

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

        rating = new Rating();
        rating.parse(value.toString());

        String s  = m.get((Integer)rating.getMovieId());
        Text value1 = new Text();
        value1.set(s+" , "+ rating.getRating());
        date = new Date(rating.getTimestamp()*1000);


        context.write(new Text(date+" "),value);


       // Stock s = parser.parse(value.toString());

       //  context.write(new Text(s.getSymbol()),value);

    }
}
