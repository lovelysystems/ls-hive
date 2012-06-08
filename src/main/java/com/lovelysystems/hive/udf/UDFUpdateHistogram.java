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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(deterministic = true)
@Description(name = "eshash",
        value = "_FUNC_(key) - Returns the elasticsearch hash")
public class UDFUpdateHistogram extends UDF {

    @SuppressWarnings("unchecked")
    public HashMap<String, HashMap<Integer, ArrayList<Integer>>> evaluate(
            HashMap<String, HashMap<Integer, ArrayList<Integer>>> l,
            HashMap<String, HashMap<Integer, ArrayList<Integer>>> r
            ) {
            if (l == null || l.isEmpty()) {
                return r;
            }
            if (r == null || r.isEmpty()) {
                return l;
            }
            
            for (Entry<String, HashMap<Integer, ArrayList<Integer>>> re : r
                    .entrySet()) {
                HashMap<Integer, ArrayList<Integer>> v;
                if (l.containsKey(re.getKey())){
                    v = l.get(re.getKey());
                 } else{
                     v = new HashMap<Integer, ArrayList<Integer>>();
                 }
                 for (Entry<Integer, ArrayList<Integer>>  ev : re.getValue()
                            .entrySet()) {
                        v.put(ev.getKey(), (ArrayList<Integer>) ev.getValue().clone());
                    }                        
                 l.put(re.getKey(), v);
                }            
            return l;
    }
    
}
