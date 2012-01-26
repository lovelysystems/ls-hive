/** 
 * Copyright 2012 Lovely Systems GmbH
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.lovelysystems.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

@Description(
        name = "arrayitem",
        value = "_FUNC_(array, pos) - Returns the element at pos with support for negative positions")
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
