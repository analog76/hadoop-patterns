package com.anand.hadoop.pattern.simple.average;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by analog76 on 5/10/14.
 */
public class AvgUtilClass implements Writable {

    private int count;
    private double sum;
    private double avg;
    private double min;
    private double max;
    private double median;
    private SortedMapWritable sort;
    private String symbol;
    private String date;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {

        return symbol;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }


    public void setAvg(double avg) {
        this.avg = avg;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public void setSort(SortedMapWritable sort) {
        this.sort = sort;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }


    public int getCount() {

        return count;
    }

    public SortedMapWritable getSort() {

        return sort;
    }

    public double getSum() {
        return sum;
    }

    public double getAvg() {
        return avg;
    }


    public double getMedian() {
        return median;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
       dataOutput.writeInt(count);
        dataOutput.writeDouble(sum);
        dataOutput.writeDouble(avg);
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(max);
        dataOutput.writeChars(symbol);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            count = dataInput.readInt();
            avg = dataInput.readDouble();
            sum= dataInput.readDouble();
            min =dataInput.readDouble();
            max = dataInput.readDouble();
            symbol=dataInput.readLine();
    }


    @Override
    public String toString() {
        return "AvgUtilClass{" +
                "count=" + count +
                ", sum=" + sum +
                ", avg=" + avg +
                ", min=" + min +
                ", max=" + max +
                ", median=" + median +
                ", sort=" + sort +
                '}';
    }


}
