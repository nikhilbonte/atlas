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

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.atlas.type.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.atlas.model.discovery.SearchParameters.ALL_CLASSIFICATIONS;
import static org.apache.atlas.model.discovery.SearchParameters.NO_CLASSIFICATIONS;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.IN;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;

public class GremlinQueryComposer {
    private static final Logger LOG                 = LoggerFactory.getLogger(GremlinQueryComposer.class);
    private static final String ISO8601_FORMAT      = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String ISO8601_DATE_FORMAT = "yyyy-MM-dd";
    private static final String REGEX_ALPHA_NUMERIC_PATTERN = "[a-zA-Z0-9]+";

    private static final ThreadLocal<DateFormat[]> DSL_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        final String formats[] = {ISO8601_FORMAT, ISO8601_DATE_FORMAT};
        DateFormat[] dfs       = new DateFormat[formats.length];
        for (int i = 0; i < formats.length; i++) {
            dfs[i] = new SimpleDateFormat(formats[i]);
            dfs[i].setTimeZone(TimeZone.getTimeZone("UTC"));
        }
        return dfs;
    });

    private final Set<String>            attributesProcessed = new HashSet<>();
    private final String                 EMPTY_STRING                = "";
    private final Lookup                 lookup;
    private final boolean                isNestedQuery;
    private final AtlasDSL.QueryMetadata queryMetadata;

    private Context                           context;
    private AtlasGraphTraversal               graphTraversal;
    private AtlasGraphTraversal.TextPredicate traversalTextPredicate;
    private boolean                           hasFrom;


    public GremlinQueryComposer(final AtlasGraphTraversal traversal, Lookup registryLookup, final AtlasDSL.QueryMetadata qmd, boolean isNestedQuery) {
        this.isNestedQuery = isNestedQuery;
        this.lookup = registryLookup;
        this.queryMetadata = qmd;

        this.graphTraversal = isNestedQuery ? traversal.startAnonymousTraversal() : traversal;
        this.traversalTextPredicate = graphTraversal.textPredicate();
    }

    public GremlinQueryComposer(final AtlasGraphTraversal graphTraversal, AtlasTypeRegistry typeRegistry, final AtlasDSL.QueryMetadata qmd) {
        this(graphTraversal, new RegistryBasedLookup(typeRegistry), qmd, false);
        this.context = new Context(lookup);

    }

    @VisibleForTesting
    GremlinQueryComposer(Lookup lookup, Context context, final AtlasDSL.QueryMetadata qmd) {
        this.isNestedQuery = false;
        this.lookup = lookup;
        this.context = context;
        this.queryMetadata = qmd;
    }

    public void addFrom(String typeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFrom(typeName={})", typeName);
        }
        hasFrom = true;

        IdentifierHelper.Info typeInfo = createInfo(typeName);

        if (context.shouldRegister(typeInfo.get())) {
            context.registerActive(typeInfo.get());

            IdentifierHelper.Info ia = createInfo(typeInfo.get());

            if (ia.isTrait()) {

                if (context != null && !context.validator.isValid(context, GremlinClause.TRAIT, ia)) {
                    return;
                }
                addClassificationClauses(ia);

                graphTraversal.has("__state", "ACTIVE").outV();
            } else {
                if (ia.hasSubtypes()) {
                    if (context != null && !context.validator.isValid(context, GremlinClause.HAS_TYPE_WITHIN, ia)) {
                        return;
                    }
                    graphTraversal.has("__typeName", P.within(ia.getSubTypes()));
                } else {
                    if (context != null && !context.validator.isValid(context, GremlinClause.HAS_TYPE, ia)) {
                        return;
                    }
                    graphTraversal.has("__typeName", (ia.getQualifiedName() != null ? ia.getQualifiedName() : ia.get()));
                }
            }

        } else {
            IdentifierHelper.Info ia = createInfo(typeInfo.get());
            introduceType(ia);
        }
    }

    private void addClassificationClauses(final IdentifierHelper.Info ia) {
        if (ia.get().equalsIgnoreCase(SearchParameters.ALL_CLASSIFICATIONS)) {
            AtlasGraphTraversal __ = graphTraversal.startAnonymousTraversal();
            graphTraversal.or(__.has("__traitNames"), __.has("__propagatedTraitNames"));
        } else if (ia.get().equalsIgnoreCase(SearchParameters.NO_CLASSIFICATIONS)) {
            graphTraversal.hasNot("__traitNames").hasNot("__propagatedTraitNames");
        } else {
            graphTraversal.outE("classifiedAs").has("__name", P.within(ia.get()));
        }
    }

    public void addFromProperty(String typeName, String attribute) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFromProperty(typeName={}, attribute={})", typeName, attribute);
        }

        if (!isNestedQuery) {
            addFrom(typeName);
        }

        IdentifierHelper.Info attrInfo = createInfo(attribute);
        if (context != null && !context.validator.isValid(context, GremlinClause.HAS_PROPERTY, attrInfo)) {
            return;
        }
        graphTraversal.has((attrInfo.getQualifiedName() == null ? attrInfo.get() : attrInfo.getQualifiedName()));
    }

    public void addIsA(String typeName, String traitName) {
        if (!isNestedQuery) {
            addFrom(typeName);
        }

        IdentifierHelper.Info traitInfo = createInfo(traitName);

       /* if (StringUtils.equals(traitName, ALL_CLASSIFICATIONS)) {
            addTrait(GremlinClause.ANY_TRAIT, traitInfo);
        } else if (StringUtils.equals(traitName, NO_CLASSIFICATIONS)) {
            addTrait(GremlinClause.NO_TRAIT, traitInfo);
        } else {
            addTrait(GremlinClause.TRAIT, traitInfo);
        }*/

        if (context != null && !context.validator.isValid(context, GremlinClause.TRAIT, traitInfo)) {
            return;
        }

        addClassificationClauses(traitInfo);

        graphTraversal.has("__state", "ACTIVE").outV();
    }

    public void addWhere(String lhs, String operator, String rhs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addWhere(lhs={}, operator={}, rhs={})", lhs, operator, rhs);
        }

        String                currentType = context.getActiveTypeName();

        IdentifierHelper.Info org         = null;
        IdentifierHelper.Info lhsI        = createInfo(lhs);
        if (!lhsI.isPrimitive()) {
            introduceType(lhsI);
            org = lhsI;
            lhsI = createInfo(lhs);
            lhsI.setTypeName(org.getTypeName());
        }

        if (!context.validator.isValidQualifiedName(lhsI.getQualifiedName(), lhsI.getRaw())) {
            return;
        }

        if (lhsI.isDate()) {
            rhs = parseDate(rhs);
        } else if (lhsI.isNumeric()) {
            rhs = parseNumber(rhs, this.context);
        }

        rhs = IdentifierHelper.get(rhs);
        Object                    normalizedRhs = getNormalizedAttrVal(lhsI, rhs);
        SearchParameters.Operator op            = SearchParameters.Operator.fromString(operator);
        if (op == SearchParameters.Operator.LIKE) {
            if (context != null && !context.validator.isValid(context, GremlinClause.TEXT_CONTAINS, lhsI)) {
                return;
            }
            graphTraversal.has(lhsI.getQualifiedName(), new P<>(traversalTextPredicate.regex(), IdentifierHelper.getFixedRegEx(rhs)));

            //TODO: enable following case
            /*final AtlasStructType.AtlasAttribute attribute = context.getActiveEntityType().getAttribute(lhsI.getAttributeName());
            final AtlasStructDef.AtlasAttributeDef.IndexType indexType = attribute.getAttributeDef().getIndexType();

            if (indexType == AtlasStructDef.AtlasAttributeDef.IndexType.STRING || !containsNumberAndLettersOnly(rhs)) {
                add(GremlinClause.STRING_CONTAINS, getPropertyForClause(lhsI), IdentifierHelper.getFixedRegEx(rhs));
            } else {
                add(GremlinClause.TEXT_CONTAINS, getPropertyForClause(lhsI), IdentifierHelper.getFixedRegEx(rhs));
            }*/
        } else if (op == SearchParameters.Operator.IN) {
            if (context != null && !context.validator.isValid(context, GremlinClause.IN, lhsI)) {
                return;
            }
            String csvRow = StringUtils.replaceEach(rhs, new String[]{"[", "]", "'"}, new String[]{"", "", ""});
            graphTraversal.has(lhsI.getQualifiedName(), P.within(csvRow.split(",")));

            //add(GremlinClause.HAS_OPERATOR, getPropertyForClause(lhsI), "within", rhs);
        } else {
            P predicate = null;
            switch (op) {
                case LT:
                    predicate = P.lt(normalizedRhs);
                    break;
                case GT:
                    predicate = P.gt(normalizedRhs);
                    break;
                case LTE:
                    predicate = P.lte(normalizedRhs);
                    break;
                case GTE:
                    predicate = P.gte(normalizedRhs);
                    break;
                case EQ:
                    predicate = P.eq(normalizedRhs);
                    break;
                case NEQ:
                    predicate = P.neq(normalizedRhs);
                    break;
            }
            if (context != null && !context.validator.isValid(context, GremlinClause.HAS_PROPERTY, lhsI)) {
                return;
            }
            graphTraversal.has(lhsI.getQualifiedName(), predicate);

            //add(GremlinClause.HAS_OPERATOR, getPropertyForClause(lhsI), op.getSymbols()[1], rhs);
        }
        // record that the attribute has been processed so that the select clause doesn't add a attr presence check
        attributesProcessed.add(lhsI.getQualifiedName());

        if (org != null && org.isReferredType()) {
            graphTraversal.dedup();
            if (org.getEdgeDirection() != null) {
                if (org.getEdgeDirection().equals(IN)){
                    graphTraversal.out(org.getEdgeLabel());
                } else {
                    graphTraversal.in(org.getEdgeLabel());
                }
            } else {
                graphTraversal.out(org.getEdgeLabel());
            }
            //add(GremlinClause.DEDUP);
            //add(GremlinClause.IN, org.getEdgeLabel());

            context.registerActive(currentType);
        }
    }

    //TODO: enable use of method
    /*private boolean containsNumberAndLettersOnly(String rhs) {
        return Pattern.matches(REGEX_ALPHA_NUMERIC_PATTERN, IdentifierHelper.removeWildcards(rhs));
    }*/

    private String parseNumber(String rhs, Context context) {
        return rhs.replace("'", "").replace("\"", "") + context.getNumericTypeFormatter();
    }

    public Set<String> getAttributesProcessed() {
        return attributesProcessed;
    }

    public void addProcessedAttributes(Set<String> attributesProcessed) {
        this.attributesProcessed.addAll(attributesProcessed);
    }

    public void addSelect(SelectClauseMetadata selectClauseMetadata) {
        process(selectClauseMetadata);

        if (CollectionUtils.isEmpty(context.getErrorList())) {
            addSelectAttrExistsCheck(selectClauseMetadata);
        }

        this.context.setSelectClauseMetadata(selectClauseMetadata);
        // If the query contains orderBy and groupBy then the transformation determination is deferred to the method processing orderBy
        /*if (!(queryMetadata.hasOrderBy() && queryMetadata.hasGroupBy())) {
            addSelectTransformation(selectClauseComposer, null, false);
        }
        this.context.setSelectClauseComposer(selectClauseComposer);*/
    }

    public GremlinQueryComposer createNestedProcessor() {
        GremlinQueryComposer qp = new GremlinQueryComposer(graphTraversal, lookup, queryMetadata, true);
        qp.context = this.context;
        return qp;
    }

    public void addFromAlias(String typeName, String alias) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addFromAlias(typeName={}, alias={})", typeName, alias);
        }

        addFrom(typeName);
        addAsClause(alias);
        context.registerAlias(alias);
    }

    public void addAsClause(String alias) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addAsClause(stepName={})", alias);
        }

        graphTraversal.as(alias);
        //add(GremlinClause.AS, alias);
    }

    public void addGroupBy(String item) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addGroupBy(item={})", item);
        }

        addGroupByClause(item);
    }

    public void addLimit(String limit, String offset, boolean global) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addLimit(limit={}, offset={}, global={})", limit, offset, global);
        }

        addLimitHelper(limit, offset, global);
        /*SelectClauseComposer scc = context.getSelectClauseComposer();
        if (scc == null) {
            addLimitHelper(limit, offset);
        } else {
            if (!scc.hasAggregators()) {
                addLimitHelper(limit, offset);
            }
        }*/
    }

    public void addAndClauses(List<AtlasGraphTraversal> clauses) {
        graphTraversal.and(clauses.toArray(new Traversal[0]));
    }

    public void addOrClauses(List<AtlasGraphTraversal> clauses) {
        graphTraversal.or(clauses.toArray(new Traversal[0]));
    }

    public AtlasGraphTraversal getGraphTraversal() {
        return graphTraversal;
    }

    public SelectClauseMetadata getSelectClause() {
        return context.selectClauseMetadata;
    }

    /*public String get() {
        close();

        boolean mustTransform = !isNestedQuery && queryMetadata.needTransformation();
        String  items[]       = getFormattedClauses(mustTransform);
        String s = mustTransform ?
                           getTransformedClauses(items) :
                           String.join(".", items);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Gremlin: {}", s);
        }

        return s;
    }*/

    public List<String> getErrorList() {
        return context.getErrorList();
    }

    public void addOrderBy(String name, boolean isDesc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderBy(name={}, isDesc={})", name, isDesc);
        }

        IdentifierHelper.Info ia = createInfo(name);
        String     qualifiedName = getQualifiedName(ia);
        addOrderByClause(qualifiedName, isDesc);
        /*if (queryMetadata.hasSelect() && queryMetadata.hasGroupBy()) {
            addSelectTransformation(this.context.selectClauseComposer, getPropertyForClause(ia), isDesc);
        } else if (queryMetadata.hasGroupBy()) {
            addOrderByClause(ia, isDesc);
            moveToLast(GremlinClause.GROUP_BY);
        } else {
            addOrderByClause(ia, isDesc);
        }*/
    }

    public long getDateFormat(String s) {

        for (DateFormat dateFormat : DSL_DATE_FORMAT.get()) {
            try {
                return dateFormat.parse(s).getTime();
            } catch (ParseException ignored) {
            }
        }

        context.validator.check(false, AtlasErrorCode.INVALID_DSL_INVALID_DATE, s);
        return -1;
    }

    public boolean hasFromClause() {
        return hasFrom;
    }

    private Object getNormalizedAttrVal(IdentifierHelper.Info attrInfo, String attrVal) {
        AtlasEntityType entityType = context.getActiveEntityType();
        String          attrName   = attrInfo.getAttributeName();
        Object          ret;

        if (entityType == null) {
            ret = attrVal;
        } else {
            AtlasType attributeType = entityType.getAttributeType(attrName);
            if (attributeType instanceof AtlasBuiltInTypes.AtlasDateType) {
                ret = ((Date) attributeType.getNormalizedValue(attrVal)).getTime();
            } else {
                ret = attributeType == null ? attrVal : attributeType.getNormalizedValue(attrVal);
            }
        }
        return ret;
    }
/*
    private String getPropertyForClause(IdentifierHelper.Info ia) {
        String vertexPropertyName = lookup.getVertexPropertyName(ia.getTypeName(), ia.getAttributeName());
        if (StringUtils.isNotEmpty(vertexPropertyName)) {
            return vertexPropertyName;
        }

        if (StringUtils.isNotEmpty(ia.getQualifiedName())) {
            return ia.getQualifiedName();
        }

        return ia.getRaw();
    }*/

    private String getQualifiedName(IdentifierHelper.Info ia) {
        return context.validator.isValidQualifiedName(ia.getQualifiedName(), ia.getRaw()) ?
                ia.getQualifiedName() : ia.getRaw();
    }

    private void addSelectAttrExistsCheck(final SelectClauseMetadata selectClauseMetadata) {
        // For each of the select attributes we need to add a presence check as well, if there's no explicit where for the same
        // NOTE: One side-effect is that the result table will be empty if any of the attributes is null or empty for the type
        String[] qualifiedAttributes = selectClauseMetadata.getAttributes();
        if (qualifiedAttributes != null && qualifiedAttributes.length > 0) {
            for (int i = 0; i < qualifiedAttributes.length; i++) {
                String                qualifiedAttribute = qualifiedAttributes[i];
                IdentifierHelper.Info idMetadata         = createInfo(qualifiedAttribute);
                // Only primitive attributes need to be checked
                if (idMetadata.isPrimitive() && !selectClauseMetadata.isAggregatorIdx(i) && !attributesProcessed.contains(qualifiedAttribute)) {
                    graphTraversal.has(qualifiedAttribute);
                    //add(GremlinClause.HAS_PROPERTY, getPropertyForClause(idMetadata));
                }
            }
            // All these checks should be done before the grouping happens (if any)
            //moveToLast(GremlinClause.GROUP_BY);
        }
    }

    private void process(SelectClauseMetadata scc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addSelect(items.length={})", scc.getItems() != null ? scc.getItems().length : 0);
        }

        if (scc.getItems() == null) {
            return;
        }

        for (int i = 0; i < scc.getItems().length; i++) {
            IdentifierHelper.Info ia = createInfo(scc.getItem(i));

            if(StringUtils.isEmpty(ia.getQualifiedName())) {
                context.getErrorList().add("Unable to find qualified name for " + ia.getAttributeName());
                continue;
            }

            if (scc.isAggregatorWithArgument(i) && !ia.isPrimitive()) {
                context.check(false, AtlasErrorCode.INVALID_DSL_SELECT_INVALID_AGG, ia.getQualifiedName());
                return;
            }

            if (!scc.getItem(i).equals(scc.getLabel(i))) {
                context.addAlias(scc.getLabel(i), ia.getQualifiedName());
            }

            scc.setAttrQualifiedName(i, getQualifiedName(ia));

            if (introduceType(ia)) {
                scc.incrementTypesIntroduced();
                /*scc.isSelectNoop = !ia.hasParts();
                if (ia.hasParts()) {
                    scc.assign(i, getPropertyForClause(createInfo(ia.get())), GremlinClause.INLINE_GET_PROPERTY);
                }*/
            } else {
                if (ia.isPrimitive()) {
                    scc.incrementPrimitiveType();
                    if (ia.isNumeric()) {
                        scc.setNumericAggregator(i);
                    }
                    scc.setIsPrimitiveAttr(i);
                }
            }
        }

        context.validator.check(!scc.hasMultipleReferredTypes(),
                                AtlasErrorCode.INVALID_DSL_SELECT_REFERRED_ATTR, Integer.toString(scc.getIntroducedTypesCount()));
        context.validator.check(!scc.hasMixedAttributes(), AtlasErrorCode.INVALID_DSL_SELECT_ATTR_MIXING);
    }

    /*private boolean hasNoopCondition(IdentifierHelper.Info ia) {
        return !ia.isPrimitive() && !ia.isAttribute() && context.hasAlias(ia.getRaw());
    }*/

    private void addLimitHelper(final String limit, final String offset, boolean globalLimit) {
        long longLimit = Long.parseLong(limit);
        graphTraversal.dedup();

        if (offset.equalsIgnoreCase("0")) {
            graphTraversal.limit(globalLimit ? Scope.global : Scope.local, longLimit);
        } else {
            addRangeClause(offset, limit, globalLimit);
        }
    }

    private String parseDate(String rhs) {
        String s = IdentifierHelper.isQuoted(rhs) ?
                           IdentifierHelper.removeQuotes(rhs) :
                           rhs;


        return String.format("'%d'", getDateFormat(s));
    }

    private boolean introduceType(IdentifierHelper.Info ia) {
        if (ia.isReferredType()) {
            if (ia.getEdgeDirection() != null) {
                if (ia.getEdgeDirection().equals(OUT)) {
                    graphTraversal.out(ia.getEdgeLabel());
                } else {
                    graphTraversal.in(ia.getEdgeLabel());
                }
            } else {
                graphTraversal.out(ia.getEdgeLabel());
            }
            context.registerActive(ia);
        }

        return ia.isReferredType();
    }

    private IdentifierHelper.Info createInfo(String actualTypeName) {
        return IdentifierHelper.create(context, lookup, actualTypeName);
    }

    private void addRangeClause(String startIndex, String endIndex, boolean global) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addRangeClause(startIndex={}, endIndex={}, global={})", startIndex, endIndex, global);
        }

        long low  = Long.parseLong(startIndex);
        long high = low + Long.parseLong(endIndex);
        graphTraversal.range(global ? Scope.global : Scope.local, low, high);
        /*if (queryMetadata.hasSelect()) {
            add(queryClauses.size() - 1, GremlinClause.RANGE, startIndex, startIndex, endIndex, startIndex, startIndex, endIndex);
        } else {
            add(GremlinClause.RANGE, startIndex, startIndex, endIndex, startIndex, startIndex, endIndex);
        }*/
    }

    private void addOrderByClause(String name, boolean descr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addOrderByClause(name={})", name, descr);
        }

        IdentifierHelper.Info ia = createInfo(name);
        if (context != null && !context.validator.isValid(context, GremlinClause.ORDER_BY, ia)) {
            return;
        }

        String key = ia.getQualifiedName() != null ? ia.getQualifiedName() : ia.get();

        graphTraversal.has(key);
        graphTraversal.order();
        if (descr) {
            graphTraversal.by(key, Order.decr);
        } else {
            graphTraversal.by(key);
        }
        //add((!descr) ? GremlinClause.ORDER_BY : GremlinClause.ORDER_BY_DESC, ia);
    }

    private void addGroupByClause(String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addGroupByClause(name={})", name);
        }

        IdentifierHelper.Info ia = createInfo(name);
        String               key = ia.getQualifiedName() != null ? ia.getQualifiedName() : ia.get();

        if (context != null && !context.validator.isValid(context, GremlinClause.GROUP_BY, ia)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Context has errors. Returning");
            }
            return;
        }
        graphTraversal.has(key);
        graphTraversal.group().by(key);
        //add(GremlinClause.GROUP_BY, ia);
    }
/*
    static class GremlinClauseValue {
        private final GremlinClause clause;
        private final String        value;

        public GremlinClauseValue(GremlinClause clause, String value) {
            this.clause = clause;
            this.value = value;
        }

        public GremlinClause getClause() {
            return clause;
        }

        public String getValue() {
            return value;
        }
    }
*/

    @VisibleForTesting
    static class Context {
        private static final AtlasStructType UNKNOWN_TYPE = new AtlasStructType(new AtlasStructDef());

        private final Lookup lookup;
        private final Map<String, String>   aliasMap = new HashMap<>();
        private AtlasType                   activeType;
        private SelectClauseMetadata        selectClauseMetadata;
        private ClauseValidator             validator;
        private String                      numericTypeFormatter = "";

        public Context(Lookup lookup) {
            this.lookup = lookup;
            validator = new ClauseValidator(lookup);
        }

        public void registerActive(String typeName) {
            if (shouldRegister(typeName)) {
                try {
                    activeType = lookup.getType(typeName);
                    aliasMap.put(typeName, typeName);
                } catch (AtlasBaseException e) {
                    validator.check(e, AtlasErrorCode.INVALID_DSL_UNKNOWN_TYPE, typeName);
                    activeType = UNKNOWN_TYPE;
                }
            }
        }

        public void registerActive(IdentifierHelper.Info info) {
            if (validator.check(StringUtils.isNotEmpty(info.getTypeName()),
                                AtlasErrorCode.INVALID_DSL_UNKNOWN_TYPE, info.getRaw())) {
                registerActive(info.getTypeName());
            } else {
                activeType = UNKNOWN_TYPE;
            }
        }

        public AtlasEntityType getActiveEntityType() {
            return (activeType instanceof AtlasEntityType) ?
                           (AtlasEntityType) activeType :
                           null;
        }

        public String getActiveTypeName() {
            return activeType.getTypeName();
        }

        public AtlasType getActiveType() {
            return activeType;
        }

        public boolean shouldRegister(String typeName) {
            return activeType == null ||
                           (activeType != null && !StringUtils.equals(getActiveTypeName(), typeName)) &&
                                   (activeType != null && !lookup.hasAttribute(this, typeName));
        }

        public void registerAlias(String alias) {
            addAlias(alias, getActiveTypeName());
        }

        public boolean hasAlias(String alias) {
            return aliasMap.containsKey(alias);
        }

        public String getTypeNameFromAlias(String alias) {
            return aliasMap.get(alias);
        }

        public boolean isEmpty() {
            return activeType == null;
        }

        public SelectClauseMetadata getSelectClauseMetadata() {
            return selectClauseMetadata;
        }

        public void setSelectClauseMetadata(SelectClauseMetadata selectClauseMetadata) {
            this.selectClauseMetadata = selectClauseMetadata;
        }

        public void addAlias(String alias, String typeName) {
            if (aliasMap.containsKey(alias)) {
                check(false, AtlasErrorCode.INVALID_DSL_DUPLICATE_ALIAS, alias, getActiveTypeName());
                return;
            }

            aliasMap.put(alias, typeName);
        }

        public List<String> getErrorList() {
            return validator.getErrorList();
        }

        public boolean error(AtlasBaseException e, AtlasErrorCode ec, String t, String name) {
            return validator.check(e, ec, t, name);
        }

        public boolean check(boolean condition, AtlasErrorCode vm, String... args) {
            return validator.check(condition, vm, args);
        }

        public void setNumericTypeFormatter(String formatter) {
            this.numericTypeFormatter = formatter;
        }

        public String getNumericTypeFormatter() {
            return this.numericTypeFormatter;
        }
    }

    private static class ClauseValidator {
        private final Lookup lookup;
        List<String> errorList = new ArrayList<>();

        public ClauseValidator(Lookup lookup) {
            this.lookup = lookup;
        }

        public boolean isValid(Context ctx, GremlinClause clause, IdentifierHelper.Info ia) {
            switch (clause) {
                case TRAIT:
                case ANY_TRAIT:
                case NO_TRAIT:
                    return check(ia.isTrait(), AtlasErrorCode.INVALID_DSL_UNKNOWN_CLASSIFICATION, ia.getRaw());

                case HAS_TYPE:
                    TypeCategory typeCategory = ctx.getActiveType().getTypeCategory();
                    return check(StringUtils.isNotEmpty(ia.getTypeName()) &&
                                         typeCategory == TypeCategory.CLASSIFICATION || typeCategory == TypeCategory.ENTITY,
                                 AtlasErrorCode.INVALID_DSL_UNKNOWN_TYPE, ia.getRaw());

                case HAS_PROPERTY:
                    return check(ia.isPrimitive(), AtlasErrorCode.INVALID_DSL_HAS_PROPERTY, ia.getRaw());

                case ORDER_BY:
                    return check(ia.isPrimitive(), AtlasErrorCode.INVALID_DSL_ORDERBY, ia.getRaw());

                case GROUP_BY:
                    return check(ia.isPrimitive(), AtlasErrorCode.INVALID_DSL_SELECT_INVALID_AGG, ia.getRaw());

                default:
                    return (getErrorList().size() == 0);
            }
        }

        public boolean check(Exception ex, AtlasErrorCode vm, String... args) {
            String[] extraArgs = getExtraSlotArgs(args, ex.getMessage());
            return check(false, vm, extraArgs);
        }

        public boolean check(boolean condition, AtlasErrorCode vm, String... args) {
            if (!condition) {
                addError(vm, args);
            }

            return condition;
        }

        public void addError(AtlasErrorCode ec, String... messages) {
            errorList.add(ec.getFormattedErrorMessage(messages));
        }

        public List<String> getErrorList() {
            return errorList;
        }

        public boolean isValidQualifiedName(String qualifiedName, String raw) {
            return check(StringUtils.isNotEmpty(qualifiedName), AtlasErrorCode.INVALID_DSL_QUALIFIED_NAME, raw);
        }

        private String[] getExtraSlotArgs(String[] args, String s) {
            String[] argsPlus1 = new String[args.length + 1];
            System.arraycopy(args, 0, argsPlus1, 0, args.length);
            argsPlus1[args.length] = s;
            return argsPlus1;
        }
    }
}
