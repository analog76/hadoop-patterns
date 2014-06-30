package com.anand.hadoop.pattern.util;

import com.anand.hadoop.pattern.simple.average.AvgUtilClass;

/**
 * Created by analog76 on 6/21/14.
 */
public class ParseLine {




    public static Double toDouble( Object value )
    {
        Double d =new Double(0);

        //    System.out.print("  "+ value.toString()+"  ");
        //    System.out.print("  "+ value.toString()+"  ");
        if( value == null ) return null;
        if( value instanceof Double )
            d = new Double((Double)value);
        //      System.out.print("  "+ value.toString()+"  ");

        if( value instanceof String )
        {
            if( "".equals( (String)value ) ) return null;
        }
        if( value instanceof Number )
            d= new Double( ((Number)value).doubleValue() );
        //       System.out.print("  "+ value.toString()+"  ");


        try {

            d = new Double(value.toString());
            //      System.out.print("  "+ value.toString()+"  ");

        }catch(NumberFormatException ne){
             d=0.0d;
        }
        return d;
    }



    public Stock parse(String line){

        String[] strVal  = line.toString().split(",");

        AvgUtilClass avg  = new AvgUtilClass();

        //exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
     //   NASDAQ,WBCO,2010-02-08,11.11,11.11,10.77,10.77,19600,10.77

        Stock s= new Stock();



            s.setSymbol(strVal[1]);
            s.setDate(strVal[2]);
            s.setOpenPrice(toDouble((strVal[3])));
            s.setHighPrice(toDouble((strVal[4])));
            s.setLowPrice(toDouble((strVal[5])));
            s.setClosePrice(toDouble((strVal[6])));
  //          s.setStockPrice(toDouble((strVal[7])));
            s.setVolume(parseLong(strVal[7]));
            s.setAdjPrice(toDouble((strVal[8])));


        return s;
    }


    public long parseLong(String s ){

        try{
            return    Long.parseLong(s);
        }catch(NumberFormatException ne){
            return 0;
        }
    }

}
