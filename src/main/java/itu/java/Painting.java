package itu.java;

/**
 * Created by analog76 on 6/21/14.
 */
public class Painting {

    String title;
    String artist;
    String medium;
    double price;
    double commision;
    double commissionRate = 20;


    public Painting(){
        this.title="Test Title";
        this.artist="Artist";
        this.medium="Medium";
        this.price=0.0d ;

    }

    public Painting(String title, String artist, String medium, double price){
        this.title=title;
        this.artist=artist;
        this.medium=medium;
        this.price=price;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setArtist(String artist) {
        this.artist = artist;
    }

    public void setMedium(String medium) {
        this.medium = medium;
    }

    public void setPrice(double price) {
        this.price = price;
        this.commision = price*commissionRate/100;
    }


    @Override
    public String toString() {
        return "Painting{" +
                "title='" + title + '\'' +
                ", artist='" + artist + '\'' +
                ", medium='" + medium + '\'' +
                ", price=" + price +
                ", commision=" + commision +
                '}';
    }
}
