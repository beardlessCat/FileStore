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
}
