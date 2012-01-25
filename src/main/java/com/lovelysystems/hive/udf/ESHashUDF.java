package com.lovelysystems.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(name = "eshash",
        value = "_FUNC_(key) - Returns the elasticsearch hash")
public class ESHashUDF extends UDF {

    private final IntWritable result = new IntWritable();

    private static long DJB_HASH(Text value) {
        long hash = 5381;

        for (int i = 0; i < value.getLength(); i++) {
            hash = ((hash << 5) + hash) + value.charAt(i);
        }
        return hash;
    }

    public IntWritable evaluate(Text key) {
        if (key == null) {
            return null;
        }
        result.set((int) DJB_HASH(key));
        return result;
    }

}
