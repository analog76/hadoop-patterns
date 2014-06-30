package com.anand.hadoop.pattern.util;

/**
 * Created by analog76 on 6/28/14.
 */
public class Item {

   /* // movie id | movie title | release date | video release date |
    IMDb URL | unknown | Action | Adventure | Animation |
    Children's | Comedy | Crime | Documentary | Drama | Fantasy |
    Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
    Thriller | War | Western |
        */



    int movieId;
    String title=" ";
    String releaseDate=null;
    String videoReleaseDate=null;
    String url;
    String unknown;
    boolean action;
    boolean adventure;
    boolean animation;
    boolean children;
    boolean comedy;
    boolean crime;
    boolean documentary;
    boolean drama;
    boolean fantasy;
    boolean noir;
    boolean horror;
    boolean musical;
    boolean mystery;
    boolean romance;
    boolean scifi;
    boolean thriller;
    boolean war;
    boolean western;

    @Override
    public String toString() {
        return "Item{" +
                "movieId=" + movieId +
                ", title='" + title + '\'' +
                ", releaseDate='" + releaseDate + '\'' +
                ", videoReleaseDate='" + videoReleaseDate + '\'' +
                ", url='" + url + '\'' +
                ", unknown='" + unknown + '\'' +
                ", action=" + action +
                ", adventure=" + adventure +
                ", animation=" + animation +
                ", children=" + children +
                ", comedy=" + comedy +
                ", crime=" + crime +
                ", documentary=" + documentary +
                ", drama=" + drama +
                ", fantasy=" + fantasy +
                ", noir=" + noir +
                ", horror=" + horror +
                ", musical=" + musical +
                ", mystery=" + mystery +
                ", romance=" + romance +
                ", scifi=" + scifi +
                ", thriller=" + thriller +
                ", war=" + war +
                ", western=" + western +
                '}';
    }

    public void parse(String line){
        String[] str=line.split("\\|");
        this.movieId=Integer.parseInt(str[0]);

        if(movieId==267){
            System.out.println("a");
        }

        this.title=str[1];
        if(str[2]!="")
            this.releaseDate=str[2];

        this.videoReleaseDate=str[3];
        this.url=str[4];
        this.unknown=str[5];

        if(Integer.parseInt(str[6])==1)
            this.action=true;

        if(Integer.parseInt(str[7])==1)
            this.adventure=true;


        if(Integer.parseInt(str[8])==1)
            this.animation=true;


        if(Integer.parseInt(str[9])==1)
            this.children=true;


        if(Integer.parseInt(str[10])==1)
            this.comedy=true;


        if(Integer.parseInt(str[11])==1)
            this.crime=true;


        if(Integer.parseInt(str[12])==1)
            this.documentary=true;


        if(Integer.parseInt(str[13])==1)
            this.drama=true;


        if(Integer.parseInt(str[14])==1)
            this.noir=true;


        if(Integer.parseInt(str[15])==1)
            this.horror=true;


        if(Integer.parseInt(str[16])==1)
            this.musical=true;


        if(Integer.parseInt(str[17])==1)
            this.mystery=true;



        if(Integer.parseInt(str[18])==1)
            this.romance=true;


        if(Integer.parseInt(str[19])==1)
            this.scifi=true;


        if(Integer.parseInt(str[20])==1)
            this.thriller=true;


        if(Integer.parseInt(str[21])==1)
            this.war=true;

        if(Integer.parseInt(str[22])==1)
            this.western=true;

    }


    public void test(){

        String str="1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0";
        parse(str);
    }


    public static void main(String[] args){
            Item i = new Item();
            i.test();
            System.out.println(i.toString());
    }


    public int getMovieId() {
        return movieId;
    }

    public String getTitle() {
        return title;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public String getVideoReleaseDate() {
        return videoReleaseDate;
    }

    public String getUrl() {
        return url;
    }

    public String getUnknown() {
        return unknown;
    }

    public boolean isAction() {
        return action;
    }

    public boolean isAdventure() {
        return adventure;
    }

    public boolean isAnimation() {
        return animation;
    }

    public boolean isChildren() {
        return children;
    }

    public boolean isComedy() {
        return comedy;
    }

    public boolean isCrime() {
        return crime;
    }

    public boolean isDocumentary() {
        return documentary;
    }

    public boolean isDrama() {
        return drama;
    }

    public boolean isFantasy() {
        return fantasy;
    }

    public boolean isNoir() {
        return noir;
    }

    public boolean isHorror() {
        return horror;
    }

    public boolean isMusical() {
        return musical;
    }

    public boolean isMystery() {
        return mystery;
    }

    public boolean isRomance() {
        return romance;
    }

    public boolean isScifi() {
        return scifi;
    }

    public boolean isThriller() {
        return thriller;
    }

    public boolean isWar() {
        return war;
    }

    public boolean isWestern() {
        return western;
    }
}
