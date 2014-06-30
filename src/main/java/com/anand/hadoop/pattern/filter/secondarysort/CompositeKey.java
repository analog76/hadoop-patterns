package com.anand.hadoop.pattern.filter.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by analog76 on 6/23/14.
 */

/**
 * This is secondary sort key.
 *  The primary key is symbol
 *  The secondary key is price.
 *  It will be sorted based on symbol and price.
 */
public class CompositeKey implements WritableComparable
{

    String symbol;
    double price;

    public CompositeKey(String symbol, double price){
        this.symbol=symbol;
        this.price=price;

    }

    public void setSymbol(String symbol){
        this.symbol=symbol;

    }

    public void setPrice(double price){
        this.price=price;
    }



    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(symbol);
        out.writeDouble(price);
    }


    @Override
    public void readFields(DataInput in) throws IOException {

        this.symbol = in.readLine();
        this.price = in.readDouble();

    }



     public int compareTo(CompositeKey ck) {
        int r = this.symbol.compareTo(ck.symbol);


        return r;
     }

    @Override
    public String toString() {
        return "CompositeKey{" +
                "symbol='" + symbol + '\'' +
                ", price=" + price +
                '}';
    }


    @Override
    public boolean equals(Object o){
        CompositeKey ck = (CompositeKey)o;
        return (this.symbol==ck.symbol);
    }

    @Override
    public int hashCode(){
        return symbol.hashCode();
    }

    @Override
    public int compareTo(Object o) {
         CompositeKey ck = (CompositeKey)o;
        if (this.symbol==ck.symbol)
                return 0;
        else
                return -1;
    }
}
