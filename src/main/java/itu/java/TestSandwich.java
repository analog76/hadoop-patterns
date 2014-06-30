package itu.java;

import java.util.Scanner;



/**
 * Created by analog76 on 6/21/14.
 */
public class TestSandwich {



    public void testX(int x, double y){

    }


    public static void main(String[] args){
        Sandwich s= new Sandwich();

        int x=5;
        double y=5.5;

        TestSandwich ts = new TestSandwich();
        ts.testX(x,x);

        Integer i=5;
        ts.testX(i,i);

        boolean exit  =false;

        Scanner scanner = new Scanner(System.in);
        String[][] arrCombo={ {"Green Sandwich "," Chicken Teriyaki Sandwich"," Beef sandwich"},
                            {"Wheat Bread","American Bread","White Bread"}
                          };


        double[][] arrPrice={{4.99,4.79},
                          {5.99,5.49 },
                            {6.99,6.49}};

        while(!exit){

            System.out.println(" Enter one of the option like 1 or 2 .");
            System.out.println(" Enter Type of sandwich   ");
            System.out.println(" 1  -  Green sandwich");
            System.out.println(" 2   -  Chicken Teriyaki sandwich");
            System.out.println(" 3   -  Beef Sandwich ");


            int sandwichType = scanner.nextInt();

            System.out.println(" Enter Type of bread   ");
            System.out.println(" 1  -  Wheat Bread");
            System.out.println(" 2   -  American Bread");
            System.out.println(" 3   -    white bread");

            int breadType = scanner.nextInt();

            System.out.println(" 4  -  Exit");

            double price =arrPrice[sandwichType-1][breadType-1];


            s = new Sandwich(arrCombo[0][sandwichType-1],arrCombo[1][breadType-1],price);
            System.out.println("****************************");
            System.out.println("****************************");
            System.out.println("");
            System.out.println("");

            System.out.println(s.toString());
            System.out.println("");
            System.out.println("");
            System.out.println("****************************");
            System.out.println("****************************");
            System.out.println("****************************");

        }
    }
}
