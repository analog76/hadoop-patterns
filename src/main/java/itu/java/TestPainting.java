package itu.java;

import java.util.Scanner;

/**
 * Created by analog76 on 6/21/14.
 */
public class TestPainting {


    public static void main(String[] args){
        Scanner s = new Scanner(System.in);

        System.out.println(" Enter the title");
        String title = s.nextLine();

        System.out.println(" Enter the Artists");
        String artist= s.nextLine();


        System.out.println(" Enter the Medium");
        String medium = s.nextLine();

        System.out.println(" Enter the Price");
        double price = s.nextDouble();


        Painting p  = new Painting();
        p.setArtist(artist);
        p.setMedium(medium);
        p.setPrice(price);
        p.setTitle(title);


        System.out.println(p.toString());
        System.out.println("**********************");
    }
}
