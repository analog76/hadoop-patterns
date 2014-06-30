package com.anand.hadoop.pattern.filter.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by analog76 on 6/28/14.
 */
public class SortComparator extends WritableComparator {

    protected SortComparator() {
        super(CustomKey.class,true);
    }
     @Override
     public int compare(WritableComparable w1, WritableComparable w2){


         CustomKey customKey =(CustomKey)w1;
         CustomKey customKey2 =(CustomKey)w2;

         int result = customKey.getSymbol().compareTo(customKey2.getSymbol());

         if(result==0){
             if(customKey.getPrice()<customKey2.getPrice())
                 result=-1;
             else if(customKey.getPrice()==customKey2.getPrice())
                 result=0;
             else
                 return 1;

         }

         return result;
     }

    /*
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

   //     System.out.println(" inside compare in sort comparator ");

        int i1 = readInt(b1, s1);
        int i2 = readInt(b2, s2);

        int comp = (i1 < i2) ? -1 : (i1 == i2) ? 0 : 1;


        if(0 != comp)
            return comp;

        int j1 = readInt(b1, s1+16);
        int j2 = readInt(b2, s2+16);
        comp = (j1 < j2) ? -1 : (j1 == j2) ? 0 : 1;

        return 0;
    }
*/

}
