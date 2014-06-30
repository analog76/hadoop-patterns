package itu.java;

/**
 * Created by analog76 on 6/21/14.
 */
public class Sandwich {

    String ingredient;
    String breadType;
    double price;

    public Sandwich(){

    }

    public Sandwich(String ingredient, String breadType,double price){
        this.ingredient=ingredient;
        this.breadType=breadType;
        this.price=price;
    }

    public String getIngredient() {
        return ingredient;
    }

    public String getBreadType() {
        return breadType;
    }

    public double getPrice() {
        return price;
    }

    public void setIngredient(String ingredient) {
        this.ingredient = ingredient;
    }

    public void setBreadType(String breadType) {
        this.breadType = breadType;
    }

    public void setPrice(double price) {
        this.price = price;
    }


    @Override
    public String toString() {
        return "Sandwich{" +
                "ingredient='" + ingredient + '\'' +
                ", breadType='" + breadType + '\'' +
                ", price=" + price +
                '}';
    }


    public void defaultSandwich(){
        this.breadType="wheat";
        this.ingredient="Green Sandwich";
        this.price=4.99;
    }



    public static void main(String[] args){
        Sandwich s = new Sandwich();
        s.defaultSandwich();
        System.out.println(s.toString());

    }



}



