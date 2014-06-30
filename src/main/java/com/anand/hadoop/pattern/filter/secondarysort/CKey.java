package com.anand.hadoop.pattern.filter.secondarysort;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by analog76 on 6/25/14.
 */
public class CKey implements Writable {

    String symbol;
    double price;

    public CKey(String symbol,double price){
        this.symbol=symbol;
        this.price=price;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(symbol);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       symbol =  dataInput.readLine();
        price = dataInput.readDouble();
    }


    @Override
    public int hashCode() {
        return symbol.hashCode()*101;
    }

    @Override
    public String toString() {
        return "CKey{" +
                "symbol='" + symbol + '\'' +
                ", price=" + price +
                '}';
    }
}
