package com.lovelysystems.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

@UDFType(deterministic = true)
@Description(name = "arrayitem", value = "_FUNC_(array, pos) - Returns the element at pos")
public class ArrayItemUDF extends GenericUDF {

    private ListObjectInspector listOI = null;
    private IntObjectInspector intOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("arrayitem() takes 2 arguments");
        }

        if (arguments[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException(
                    "arrayitem() takes an array as a parameter");
        }

        listOI = (ListObjectInspector) arguments[0];
        intOI = (IntObjectInspector) arguments[1];

        return listOI.getListElementObjectInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        if (arguments[0] == null || arguments[1] == null) {
            return null;
        }
        Object l = arguments[0].get();
        int apos = intOI.get(arguments[1].get());
        if (l == null) {
            return null;
        }
        int size = listOI.getListLength(l);
        if (size < 1) {
            return null;
        }
        if (apos < 0) {
            apos += size;
        }
        if (apos >= size) {
            return null;
        }
        return listOI.getListElement(l, apos);

    }

    @Override
    public String getDisplayString(String[] children) {
        return ArrayItemUDF.class.getCanonicalName() + "(" + children[0] + ", "
                + children[1] + ")";
    }
}
