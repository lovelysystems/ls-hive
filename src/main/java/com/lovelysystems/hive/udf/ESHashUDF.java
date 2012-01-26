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
