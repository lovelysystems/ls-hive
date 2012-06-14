package com.lovelysystems.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

/**
 * Find the value of the largest argument.
 */
@Description(name = "udfgreatest",
        value = "_FUNC_(...) - Find the value of the largest element.  Unlike MAX, GREATEST finds the row-size maximum element (rather than the column-wise).  NULLs are ignored except when all arguments are NULL in which case NULL is returned.",
        extended = "Example:\n"
                + "  > SELECT GREATEST(foo, bar) FROM users;\n")
public class GreatestUDF extends GenericUDF {

    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private ObjectInspector compareType;

    @Override
    public ObjectInspector initialize(ObjectInspector ... arguments) throws UDFArgumentException {

        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver();
        for (int i = 0; i < arguments.length; i++) {
            if (!returnOIResolver.update(arguments[i])) {
                throw new UDFArgumentTypeException(i,
                        "The expressions after GREATEST should all have the same type: \""
                                + returnOIResolver.get().getTypeName()
                                + "\" is expected but \"" + arguments[i].getTypeName()
                                + "\" is found");
            }
        }
        compareType = returnOIResolver.get();
        return compareType;
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject ... arguments) throws HiveException {

        Object maxVal = null;
        for (int i = 0; i < arguments.length; i++) {
            GenericUDF.DeferredObject val = arguments[i];
            if (val != null) {
                if (maxVal == null || ObjectInspectorUtils.compare(val.get(), compareType, maxVal, compareType) > 0) {
                    maxVal = val.get();
                }
            }
        }
        return returnOIResolver.convertIfNecessary(maxVal, compareType);
    }

    @Override
    public String getDisplayString(String[] strings) {
        StringBuilder sb = new StringBuilder();
        sb.append("GREATEST(");
        if (strings.length > 0) {
            sb.append(strings[0]);
            for (int i = 1; i < strings.length; i++) {
                sb.append(",");
                sb.append(strings[i]);
            }
        }
        sb.append(")");
        return sb.toString();    }
}
