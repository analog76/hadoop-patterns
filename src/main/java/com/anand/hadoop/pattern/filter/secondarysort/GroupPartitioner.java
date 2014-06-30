package com.anand.hadoop.pattern.filter.secondarysort;

import org.apache.hadoop.io.RawComparator;

/**
 * Created by analog76 on 6/23/14.
 */
public class GroupPartitioner implements RawComparator<CompositeKey> {
    @Override
    public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int i4) {
        return 0;
    }

    @Override
    public int compare(CompositeKey compositeKey, CompositeKey compositeKey2) {

        if(compositeKey.symbol.equalsIgnoreCase(compositeKey2.symbol)){
            if(compositeKey.price < compositeKey2.price){
                return -1;

            }else if (compositeKey.price > compositeKey2.price){
                return 1;
            }else
                return 0;

        }else{
            return -1;
        }
    }
}
