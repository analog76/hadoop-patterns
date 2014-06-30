package com.anand.hadoop.pattern.filter.secondarysort;

import org.apache.hadoop.io.RawComparator;

/**
 * Created by analog76 on 6/26/14.
 */
public class GroupingComparator implements RawComparator<CustomKey> {
    @Override
    public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int i4) {
        return 0;
    }

    @Override
    public int compare(CustomKey customKey, CustomKey customKey2) {

        int result = customKey.symbol.compareTo(customKey2.symbol);

        if(result==0){
            if ( customKey.price==customKey2.price){
                result =0;
            }else if ( customKey.price > customKey2.price){
                result =1;
            }else
                result =-1;
        }

        return result;
    }
}
