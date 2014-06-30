package com.anand.hadoop.pattern.util;

/**
 * Created by analog76 on 6/29/14.
 */
public class Rating {

    //userid,movieid,rating,timestamp

    int userId;
    int movieId;
    int rating;
    long timestamp;

    public int getUserId() {
        return userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public int getRating() {
        return rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void parse(String line){
     //   943	595	2	875502597

           String[] str=line.split("\t");
           this.userId=Integer.parseInt(str[0]);
           this.movieId=Integer.parseInt(str[1]);
           this.rating=Integer.parseInt(str[2]);
           this.timestamp=Long.parseLong(str[3]);



    }


    public static void main(String[] args){
        String line="943\t595\t2\t875502597";
      //  line="943	595	2	875502597";

        Rating r = new Rating();
        r.parse(line);
        System.out.println(r.toString());

    }

    @Override
    public String toString() {
        return "Rating{" +
                "userId=" + userId +
                ", movieId=" + movieId +
                ", rating=" + rating +
                ", timestamp=" + timestamp +
                '}';
    }
}
