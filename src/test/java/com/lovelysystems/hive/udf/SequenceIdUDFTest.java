package com.lovelysystems.hive.udf;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SequenceIdUDFTest {

    @Test
    public void testEvaluate() {
        
        // we simulate 2 tasks 
        
        // one with task id 14
        SequenceIdUDF udf1 = new SequenceIdUDF();
        udf1.setWorkdir("/x/y/job_201203272327_11693/attempt_201203272327_11693_m_000014_0/work");
        
        // and one with task id 9
        SequenceIdUDF udf2 = new SequenceIdUDF();
        udf2.setWorkdir("/x/y/job_201203272327_11693/attempt_201203272327_11693_m_000009_0/work");
        

        // every tasks evaluates 2 rows
        
        String res1 = Long.toBinaryString(udf1.evaluate(null));
        String res2 = Long.toBinaryString(udf1.evaluate(null));
        
        String res3 = Long.toBinaryString(udf2.evaluate(null));
        String res4 = Long.toBinaryString(udf2.evaluate(null));
        
        // the timestamp bits are the same in the same task
        assertEquals(
                res1.substring(0, res1.length()-32),
                res2.substring(0, res2.length()-32));
        
        assertEquals(
                res3.substring(0, res3.length()-32),
                res4.substring(0, res4.length()-32));
        
        
        // the taskid bits are the same in the same tasks
        assertEquals("000000001110", res1.substring(res1.length()-32, res1.length()-20));
        assertEquals("000000001110", res2.substring(res2.length()-32, res1.length()-20));
        
        //  but different for other tasks
        assertEquals("000000001001", res3.substring(res3.length()-32, res3.length()-20));
        assertEquals("000000001001", res4.substring(res4.length()-32, res4.length()-20));
        
        
        // the sequence is increased and started at zero for every task
        assertEquals("00000000000000000000", res1.substring(res1.length()-20, res1.length()));
        assertEquals("00000000000000000001", res2.substring(res2.length()-20, res2.length()));
        
        assertEquals("00000000000000000000", res3.substring(res3.length()-20, res3.length()));
        assertEquals("00000000000000000001", res4.substring(res4.length()-20, res4.length()));
        

    }
    
    @Test 
    public void testLocalMode() {
        // if no work directory is defined the task id is set to one, because 
        // it is assumed that the udf is running in local mode       
        SequenceIdUDF udf1 = new SequenceIdUDF();
        String res1 = Long.toBinaryString(udf1.evaluate(null));
        assertEquals("000000000001", res1.substring(res1.length()-32, res1.length()-20));
    }

}
