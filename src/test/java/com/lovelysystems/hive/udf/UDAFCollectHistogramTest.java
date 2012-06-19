package com.lovelysystems.hive.udf;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class UDAFCollectHistogramTest {

    /*
     * Collect counts of two rows.
     */
    @Test
    public void testCollectDifferentTS() {

        UDAFCollectHistogram.UDAFCollectHistogramEvaluator evaluator = new UDAFCollectHistogram.UDAFCollectHistogramEvaluator();

        evaluator.iterate("http://twitter.com/quodt", 1, "1 3 5");
        evaluator.iterate("http://twitter.com/quodt", 2, "2 5 9");

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> res = evaluator.terminate();

        assertEquals("{http://twitter.com/quodt={1=[1, 3, 5], 2=[2, 5, 9]}}", res.toString());

    }

    /**
     * While collecting multiple counts of the same timestamp the first value will win but in practice it doesn't
     * matter if the first or the last value will win since it is very unlikely that there are different values for the
     * same timestamp.
     */
    @Test
    public void testCollectSameTS() {

        UDAFCollectHistogram.UDAFCollectHistogramEvaluator evaluator = new UDAFCollectHistogram.UDAFCollectHistogramEvaluator();

        evaluator.iterate("http://twitter.com/quodt", 1, "1 3 5");
        evaluator.iterate("http://twitter.com/quodt", 1, "2 4 6");

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> res = evaluator.terminate();

        assertEquals("{http://twitter.com/quodt={1=[1, 3, 5]}}", res.toString());

    }

}
