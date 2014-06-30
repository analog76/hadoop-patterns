package com.anand.hadoop.pattern.filter.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by analog76 on 6/26/14.
 */
public class CustomKey implements WritableComparable {

    String symbol;
    double price;

    public void setSymbol(String symbol){
        this.symbol=symbol;
    }

    public void setPrice(double price){
        this.price=price;
    }

    @Override
    public int compareTo(Object o) {
        CustomKey ck =(CustomKey)o;

        if(symbol.equalsIgnoreCase(ck.symbol)){
             if(price == ck.price)
                 return 0;
             else if (price > ck.price)
                  return 1;
            else
                    return -1;
        }

        return -1;



    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeChars(symbol);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        price =dataInput.readDouble();
        symbol=dataInput.readLine();
    }



    @Override
    public int hashCode(){

        return symbol.hashCode() ^ (int) Double.doubleToLongBits(price);
    }


    @Override
    public boolean equals(Object o){


         CustomKey  ck = (CustomKey)o;
         return this.symbol==ck.symbol  && this.price==ck.price;

    }


    @Override
    public String toString() {
        return "CustomKey{" +
                "symbol='" + symbol + '\'' +
                ", price=" + price +
                '}';
    }
}
