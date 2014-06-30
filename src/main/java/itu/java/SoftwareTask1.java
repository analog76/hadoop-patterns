package itu.java;

/**
 * Created by analog76 on 6/22/14.
 */
public class SoftwareTask1 {


    private long MEXICO_POPULATION=114000000;
    private long US_POPULATION=312000000;

    private double MEXICO_GROWTH=1.01;
    private double US_GROWTH=0.15;


    public static void main(String[] args){

        SoftwareTask1 st=  new SoftwareTask1();
        st.findPopulation();


    }



    public void findPopulation(){

        int count=0;
        while(MEXICO_POPULATION < US_POPULATION) {
            US_POPULATION = US_POPULATION - (long)(US_POPULATION * US_GROWTH/100);
            MEXICO_POPULATION = MEXICO_POPULATION +(long) (MEXICO_POPULATION * MEXICO_GROWTH/100);
            System.out.println(" population after year "+ count + " "+ US_POPULATION + " "  +MEXICO_POPULATION);
            count=count+1;
        }

        System.out.println(" population exceeds after year "+ count + " "+ US_POPULATION + " "  +MEXICO_POPULATION);


    }
}
