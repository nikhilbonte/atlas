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

package org.apache.atlas.query;

import org.apache.commons.lang.ArrayUtils;

public class SelectClauseMetadata {
    private static final String COUNT_STR = "count";
    private static final String MIN_STR   = "min";
    private static final String MAX_STR   = "max";
    private static final String SUM_STR   = "sum";

    private String[]         labels; // Think of this as the table headers
    private String[]         items;  //
    private String[]         attributes; // Qualified names
    private boolean[]        isNumericAggregator;
    private boolean[]        isPrimitiveAttr;
    private AggregatorFlag[] aggregatorFlags;

    private int aggCount             = 0;
    private int introducedTypesCount = 0;
    private int primitiveTypeCount   = 0;

    public SelectClauseMetadata(int count) {
        labels = new String[count];
        items = new String[count];
        attributes = new String[count];
        isNumericAggregator = new boolean[count];
        isPrimitiveAttr = new boolean[count];
        aggregatorFlags = new AggregatorFlag[count];

    }

    public static boolean isKeyword(String s) {
        return COUNT_STR.equals(s) ||
                       MIN_STR.equals(s) ||
                       MAX_STR.equals(s) ||
                       SUM_STR.equals(s);
    }

    public String[] getItems() {
        return items;
    }

    public void setAttrQualifiedName(int currentIndex, String qualifiedName) {
        attributes[currentIndex] = qualifiedName;
    }

    public String[] getAttributes() {
        return attributes;
    }

    public String[] getLabels() {
        return labels;
    }

    public boolean onlyAggregators() {
        return hasAggregators() && aggCount == items.length;
    }

    public boolean hasAggregators() {
        return aggCount > 0;
    }

    public String getItem(int i) {
        return items[i];
    }

    public void setItem(int i, String item) {
        assert i >= 0 && i < items.length;
        items[i] = item;
    }

    public String getAttribute(int i) {
        return attributes[i];
    }

    public void setAttribute(int i, String attribute) {
        assert i >= 0 && i < attributes.length;
        attributes[i] = attribute;
    }

    public String getLabel(int i) {
        return labels[i];
    }

    public void setLabel(int i, String label) {
        assert i >= 0 && i < labels.length;
        labels[i] = label;
    }


    public boolean isAggregatorIdx(int idx) {
        return getAggregatorFlag(idx) != AggregatorFlag.NONE;
    }

    public boolean isNumericAggregator(int idx) {
        return !ArrayUtils.isEmpty(isNumericAggregator) && (idx < isNumericAggregator.length && isNumericAggregator[idx]);
    }

    public boolean isPrimitiveAttribute(int idx) {
        return !ArrayUtils.isEmpty(isPrimitiveAttr) && (idx < isPrimitiveAttr.length && isPrimitiveAttr[idx]);
    }

    public boolean isAggregatorWithArgument(int i) {
        return isAggregatorIdx(i) && getAggregatorFlag(i) != AggregatorFlag.COUNT;
    }

    public void incrementTypesIntroduced() {
        introducedTypesCount++;
    }

    public int getIntroducedTypesCount() {
        return introducedTypesCount;
    }

    public void incrementPrimitiveType() {
        primitiveTypeCount++;
    }

    public boolean hasMultipleReferredTypes() {
        return getIntroducedTypesCount() > 1;
    }

    public boolean hasMixedAttributes() {
        return getIntroducedTypesCount() > 0 && getPrimitiveTypeCount() > 0;
    }

    public void setNumericAggregator(int idx) {
        assert idx >= 0 && idx < isNumericAggregator.length;
        isNumericAggregator[idx] = true;
    }

    public boolean[] getIsPrimitiveAttr() {
        return isPrimitiveAttr;
    }

    public void setIsPrimitiveAttr(int idx) {
        assert idx >= 0 && idx < isPrimitiveAttr.length;
        isPrimitiveAttr[idx] = true;
    }

    public boolean isSelectNoop() {
        return getPrimitiveTypeCount() == 0 && getIntroducedTypesCount() == 0 && aggCount == 0;
    }

    public AggregatorFlag[] getAggregatorFlags() {
        return aggregatorFlags;
    }

    public void setAggregatorFlag(int idx, AggregatorFlag flag) {
        assert idx >= 0 && idx < labels.length;
        aggregatorFlags[idx] = flag;
        if (flag != AggregatorFlag.NONE) {
            aggCount++;
        }
    }

    public AggregatorFlag getAggregatorFlag(int idx) {
        if (aggregatorFlags != null && idx >= 0 && idx < aggregatorFlags.length) {
            return aggregatorFlags[idx];
        } else {
            return AggregatorFlag.NONE;
        }
    }

    public boolean isSumIdx(int idx) {
        return getAggregatorFlag(idx) == AggregatorFlag.SUM;
    }

    public boolean isMinIdx(int idx) {
        return getAggregatorFlag(idx) == AggregatorFlag.MIN;
    }

    public boolean isMaxIdx(int idx) {
        return getAggregatorFlag(idx) == AggregatorFlag.MAX;
    }

    public boolean isCountIdx(int idx) {
        return getAggregatorFlag(idx) == AggregatorFlag.COUNT;
    }

    private int getPrimitiveTypeCount() {
        return primitiveTypeCount;
    }

    public enum AggregatorFlag {
        NONE, COUNT, MIN, MAX, SUM
    }
}