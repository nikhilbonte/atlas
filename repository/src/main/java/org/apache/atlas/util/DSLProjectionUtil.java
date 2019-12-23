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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.query.GremlinTraversal;
import org.apache.atlas.query.SelectClauseMetadata;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.model.discovery.AtlasSearchResult.AttributeSearchResult;

public class DSLProjectionUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DSLProjectionUtil.class);

    public static AtlasSearchResult createProjection(final GremlinTraversal gremlinTraversal,
                                                     final EntityGraphRetriever entityRetriever,
                                                     final Collection<AtlasVertex> resultList) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Post-processing for select clause");
        }

        AtlasSearchResult     ret                   = new AtlasSearchResult();
        SelectClauseMetadata  selectClauseMetadata  = gremlinTraversal.getSelectClauseMetadata();
        AttributeSearchResult attributeSearchResult = new AttributeSearchResult();

        attributeSearchResult.setName(Arrays.stream(selectClauseMetadata.getLabels()).collect(Collectors.toList()));

        Collection<List<Object>> values = getProjectionRows(resultList, selectClauseMetadata, entityRetriever);

        if (values instanceof List) {
            attributeSearchResult.setValues((List) values);
        } else if (values instanceof Set) {
            attributeSearchResult.setValues(new ArrayList<>(values));
        }

        ret.setAttributes(attributeSearchResult);

        return ret;
    }

    public static AtlasSearchResult createProjection(final GremlinTraversal gremlinTraversal,
                                                     final EntityGraphRetriever entityRetriever,
                                                     final Map<String, Collection<AtlasVertex>> resultMap) throws AtlasBaseException {
        AtlasSearchResult     ret                   = new AtlasSearchResult();
        SelectClauseMetadata  selectClauseMetadata  = gremlinTraversal.getSelectClauseMetadata();
        AttributeSearchResult attributeSearchResult = new AttributeSearchResult();

        attributeSearchResult.setName(Arrays.stream(selectClauseMetadata.getLabels()).collect(Collectors.toList()));

        List<List<Object>> values = new ArrayList<>();
        for (Collection<AtlasVertex> value : resultMap.values()) {
            values.addAll(getProjectionRows(value, selectClauseMetadata, entityRetriever, true));
        }

        attributeSearchResult.setValues(values);

        ret.setAttributes(attributeSearchResult);

        return ret;
    }

    private static Collection<List<Object>> getProjectionRows(final Collection<AtlasVertex> vertices,
                                                              final SelectClauseMetadata selectClauseMetadata,
                                                              final EntityGraphRetriever entityRetriever) throws AtlasBaseException {
        return getProjectionRows(vertices, selectClauseMetadata, entityRetriever, selectClauseMetadata.onlyAggregators());
    }

    private static Collection<List<Object>> getProjectionRows(Collection<AtlasVertex> vertices,
                                                              SelectClauseMetadata selectClauseMetadata,
                                                              EntityGraphRetriever entityRetriever,
                                                              boolean unique) throws AtlasBaseException {
        // Form a projection
        Collection<List<Object>> values = unique ? new HashSet<>() : new ArrayList<>();

        for (AtlasVertex vertex : vertices) {

            // Construct projection tuple
            List<Object> row = new ArrayList<>();

            for (int idx = 0; idx < selectClauseMetadata.getLabels().length; idx++) {
                if (selectClauseMetadata.isMinIdx(idx)) {
                    row.add(computeMin(vertices, selectClauseMetadata, idx));
                } else if (selectClauseMetadata.isMaxIdx(idx)) {
                    row.add(computeMax(vertices, selectClauseMetadata, idx));
                } else if (selectClauseMetadata.isCountIdx(idx)) {
                    row.add(vertices.size());
                } else if (selectClauseMetadata.isSumIdx(idx)) {
                    row.add(computeSum(vertices, selectClauseMetadata, idx));
                } else {
                    if (selectClauseMetadata.isPrimitiveAttribute(idx)) {
                        String propertyName = selectClauseMetadata.getAttribute(idx);
                        row.add(vertex.getProperty(propertyName, Object.class));
                    } else {
                        row.add(entityRetriever.toAtlasEntityHeaderWithClassifications(vertex));
                    }
                }
            }

            values.add(row);
        }
        return values;
    }

    private static Number computeSum(final Collection<AtlasVertex> vertices, final SelectClauseMetadata selectClauseMetadata, final int idx) {
        if (selectClauseMetadata.isNumericAggregator(idx)) {
            String propertyName = selectClauseMetadata.getAttribute(idx);

            // This has a potential to overflow the buffer
            double sumDouble = 0;
            for (AtlasVertex vertex : vertices) {
                Number value = vertex.getProperty(propertyName, Number.class);
                sumDouble += value.doubleValue();
            }
            return sumDouble;
        } else {
            // Sum for Non-numeric attr
            return 0;
        }
    }

    private static Object computeMax(final Collection<AtlasVertex> vertices, final SelectClauseMetadata selectClauseMetadata, final int idx) {
        final Object max;
        String       propertyName = selectClauseMetadata.getAttribute(idx);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing select with max for property={}", propertyName);
        }

        if (selectClauseMetadata.isNumericAggregator(idx)) {
            max = Collections.max(vertices, new AtlasNumericTypeComparator(propertyName));
        } else {
            max = Collections.max(vertices.stream().map(v -> v.getProperty(propertyName, String.class)).filter(Objects::nonNull)
                                          .collect(Collectors.toList()), String.CASE_INSENSITIVE_ORDER);
        }
        return max;
    }

    private static Object computeMin(final Collection<AtlasVertex> vertices, final SelectClauseMetadata selectClauseMetadata, final int idx) {
        final Object min;
        String       propertyName = selectClauseMetadata.getAttribute(idx);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing select with min for property={}", propertyName);
        }

        if (selectClauseMetadata.isNumericAggregator(idx)) {
            min = Collections.min(vertices, new AtlasNumericTypeComparator(propertyName));
        } else {
            min = Collections.min(vertices.stream().map(v -> v.getProperty(propertyName, String.class)).filter(Objects::nonNull)
                                          .collect(Collectors.toList()), String.CASE_INSENSITIVE_ORDER);
        }

        return min;
    }
}