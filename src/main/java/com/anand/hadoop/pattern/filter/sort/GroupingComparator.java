package com.anand.hadoop.pattern.filter.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by analog76 on 6/28/14.
 */
public class GroupingComparator extends WritableComparator {

    protected GroupingComparator() {
        super(CustomKey.class,true);
    }

     @Override
     public int compare(WritableComparable w1, WritableComparable w2) {

         CustomKey c1 =(CustomKey)w1;
         CustomKey c2 =(CustomKey)w2;

        return  c1.getSymbol().compareTo(c2.getSymbol());
    }


/*
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {


        int i1 = readInt(b1, s1);
        int i2 = readInt(b2, s2);

        int comp = (i1 < i2) ? -1 : (i1 == i2) ? 0 : 1;
        return comp;

    }
*/

}
