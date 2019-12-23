/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

import org.apache.atlas.repository.graphdb.AtlasVertex;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;

public class AtlasNumericTypeComparator implements Comparator<AtlasVertex> {

    private String propertyName;

    public AtlasNumericTypeComparator(final String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public int compare(final AtlasVertex o1, final AtlasVertex o2) {

        Object p1 = o1 == null ? null : o1.getProperty(propertyName, Object.class);
        Object p2 = o2 == null ? null : o2.getProperty(propertyName, Object.class);

        if (p1 instanceof Byte && p2 instanceof Byte) {
            return ((Byte) p1).compareTo((Byte) p2);
        }
        if (p1 instanceof Short && p2 instanceof Short) {
            return ((Short) p1).compareTo((Short) p2);
        }
        if (p1 instanceof Integer && p2 instanceof Integer) {
            return ((Integer) p1).compareTo((Integer) p2);
        }
        if (p1 instanceof Float && p2 instanceof Float) {
            return ((Float) p1).compareTo((Float) p2);
        }
        if (p1 instanceof Double && p2 instanceof Double) {
            return ((Double) p1).compareTo((Double) p2);
        }
        if (p1 instanceof Long && p2 instanceof Long) {
            return ((Long) p1).compareTo((Long) p2);
        }
        if (p1 instanceof BigInteger && p2 instanceof BigInteger) {
            return ((BigInteger) p1).compareTo((BigInteger) p2);
        }
        if (p1 instanceof BigDecimal && p2 instanceof BigDecimal) {
            return ((BigDecimal) p1).compareTo((BigDecimal) p2);
        }

        return 0;
    }
}