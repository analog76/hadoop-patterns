package com.anand.hadoop.pattern.filter.secondarysort;

import org.apache.hadoop.io.RawComparator;

/**
 * Created by analog76 on 6/24/14.
 */
public class KeyComparator implements RawComparator<CustomKey> {


    @Override
    public int compare(CustomKey customKey, CustomKey customKey2) {
        return customKey.symbol.compareTo( customKey2.symbol);
    }

    @Override
    public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int i4) {
        return 0;
    }
}
