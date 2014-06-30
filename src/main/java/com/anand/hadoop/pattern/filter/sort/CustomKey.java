package com.anand.hadoop.pattern.filter.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by analog76 on 6/28/14.
 */
public class CustomKey implements WritableComparable<CustomKey> {

    private String symbol;
    private double price;



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(symbol);
        dataOutput.writeDouble(price);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        symbol=dataInput.readUTF();
        price = dataInput.readDouble();
    }


    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getSymbol() {
        return symbol;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public int compareTo(CustomKey customKey) {
        if(this.symbol.equalsIgnoreCase(customKey.symbol)){
            return 1;
        }
        return 0;
    }


    @Override
    public int hashCode() {
        final int prime = 31;

        int result = 1;
        result = prime * this.symbol.hashCode();
        return result;
    }


    @Override
    public String toString() {
        return "CustomKey{" +
                "symbol='" + symbol + '\'' +
                ", price=" + price +
                '}';
    }
}
