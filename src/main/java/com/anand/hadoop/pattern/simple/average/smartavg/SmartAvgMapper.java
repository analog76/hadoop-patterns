package com.anand.hadoop.pattern.simple.average.smartavg;

import com.anand.hadoop.pattern.simple.average.AvgUtilClass;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by analog76 on 5/31/14.
 */
public class SmartAvgMapper  extends Mapper<LongWritable, Text, Text, SortedMapWritable> {



    Text txtKey= null;
    LongWritable l ;
    DoubleWritable d;
    Object s  =null;
    double d1;
    Double dd= null;
    SortedMapWritable sortedMap = new SortedMapWritable();
    public void map(LongWritable key,Text value,Context ctx)throws IOException, InterruptedException{
        //     System.out.println(" value is "+ value.toString());
        String[] strVal  = value.toString().split(",");
        //       System.out.println(" strVal "+strVal[1]);
        if(strVal[1]!=null) {
            txtKey=new Text(strVal[1].trim());
            //     System.out.println(" strVal "+strVal[1]+" "+s);

            try {
                s=strVal[5];

                dd = toDouble(s);
//                System.out.println(" strVal "+strVal[1]+" "+dd.doubleValue());

                //        d = new DoubleWritable(dd.doubleValue());
                AvgUtilClass util = new AvgUtilClass();
                util.setCount(1);
                util.setSum(dd.doubleValue());
                //          System.out.println(" value is " + util.toString());

                sortedMap.put(txtKey,util);
                ctx.write(txtKey, sortedMap);
            }catch(NumberFormatException npe){
                npe.printStackTrace();
            }
        }


    }


    public static Double toDouble( Object value ) throws NumberFormatException
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

        d = new Double( value.toString() );
        //      System.out.print("  "+ value.toString()+"  ");

        return d;
    }
}
