package itu.java;

import java.util.Scanner;

/**
 * Created by analog76 on 6/21/14.
 */
public class PaintingCost {

    int  gallosWithArea =350;
    int costPerGallon=32;

    public double  calculateArea(double length,double width, double height){
        double area =   length*  height*2+ width*height*2;
        return area;
    }

    public double calculateGallon(double area){

        return area/gallosWithArea;
    }

    public double findCost(double gallon){

        return gallon*costPerGallon;
    }


    public void execute(double length,double width,double height){
            double area = calculateArea(length,width,height);
            System.out.println(" Total area is "+area);

            double gallon = calculateGallon(area);
            System.out.println(" Total gallon required is "+gallon);

            double cost = findCost(gallon);

            System.out.println(" ************");
            System.out.println(" Total cost is "+ cost);

        System.out.println(" ************");
    }


    public static void main(String[] args){
        Scanner s = new Scanner(System.in);

        System.out.println(" Enter length ");
        double length = s.nextDouble();

        System.out.println(" Enter width ");
        double width = s.nextDouble();


        System.out.println(" Enter height ");
        double height = s.nextDouble();

        PaintingCost p = new PaintingCost();
        p.execute(length,width,height);
    }

}
