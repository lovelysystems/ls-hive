package com.lovelysystems.hive.udf;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class UnescapeXMLTest {

    @Test
    public void testText() {

        assertEquals("abc", UnescapeXMLUDF.unescapeXML("abc"));
        assertEquals("a>b>c", UnescapeXMLUDF.unescapeXML("a&gt;b&gt;c"));
        assertEquals("ÄÄ", UnescapeXMLUDF.unescapeXML("ÄÄ"));
    }

}
