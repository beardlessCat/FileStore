package com.bigyj.util;

import java.text.NumberFormat;

public class MsgUtils {
    public static long tagsString2tagsCode(final String tags) {
        if (null == tags || tags.length() == 0) { return 0; }
        int tagCode = tags.hashCode();
        return tagCode;
    }
    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }
    public static int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }
}
