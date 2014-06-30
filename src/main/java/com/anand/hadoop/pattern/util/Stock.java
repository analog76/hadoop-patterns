package com.anand.hadoop.pattern.util;

/**
 * Created by analog76 on 6/23/14.
 */
public class Stock {

    private String symbol;
    private String date;
    private double stockPrice;
    private double openPrice;
    private double highPrice;
    private double lowPrice;
    private double closePrice;
    private long volume;
    private double adjPrice;

    public String getSymbol() {
        return symbol;
    }

    public String getDate() {
        return date;
    }

    public double getStockPrice() {
        return stockPrice;
    }

    public double getOpenPrice() {
        return openPrice;
    }

    public double getHighPrice() {
        return highPrice;
    }

    public double getLowPrice() {
        return lowPrice;
    }

    public double getClosePrice() {
        return closePrice;
    }

    public long getVolume() {
        return volume;
    }

    public double getAdjPrice() {
        return adjPrice;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setStockPrice(double stockPrice) {
        this.stockPrice = stockPrice;
    }

    public void setOpenPrice(double openPrice) {
        this.openPrice = openPrice;
    }

    public void setHighPrice(double highPrice) {
        this.highPrice = highPrice;
    }

    public void setLowPrice(double lowPrice) {
        this.lowPrice = lowPrice;
    }

    public void setClosePrice(double closePrice) {
        this.closePrice = closePrice;
    }

    public void setVolume(long volume) {
        this.volume = volume;
    }

    public void setAdjPrice(double adjPrice) {
        this.adjPrice = adjPrice;
    }


    @Override
    public String toString() {
        return "Stock{" +
                "symbol='" + symbol + '\'' +
                ", date='" + date + '\'' +
                ", stockPrice=" + stockPrice +
                ", openPrice=" + openPrice +
                ", highPrice=" + highPrice +
                ", lowPrice=" + lowPrice +
                ", closePrice=" + closePrice +
                ", volume=" + volume +
                ", adjPrice=" + adjPrice +
                '}';
    }
}
