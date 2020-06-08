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
package org.apache.atlas.repository.tagstatus;

import com.vividsolutions.jts.util.Assert;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.impexp.ImportService;
import org.apache.atlas.repository.impexp.ZipFileResourceTestUtils;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.testng.SkipException;
import org.testng.annotations.*;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.apache.atlas.AtlasErrorCode.PROPAGATED_CLASSIFICATION_REMOVAL_NOT_SUPPORTED;
import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_ADD;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_UPDATE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_DELETE;
import static org.apache.atlas.model.instance.AtlasClassification.Status.*;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runImportWithNoParameters;
import static org.apache.atlas.utils.TestLoadModelUtils.loadModelFromJson;
import static org.testng.Assert.*;


@Guice(modules = TestModules.TestOnlyModule.class)
public class ClassificationStatusTest {
    private static final String HDFS_PATH_EMPLOYEES     = "HDFS_PATH_EMPLOYEES";
    private static final String EMPLOYEES1_TABLE        = "EMPLOYEES1_TABLE";
    private static final String EMPLOYEES2_TABLE        = "EMPLOYEES2_TABLE";
    private static final String EMPLOYEES_UNION_TABLE   = "EMPLOYEES_UNION_TABLE";
    private static final String EMPLOYEES1_PROCESS      = "EMPLOYEES1_PROCESS";
    private static final String EMPLOYEES2_PROCESS      = "EMPLOYEES2_PROCESS";
    private static final String EMPLOYEES_UNION_PROCESS = "EMPLOYEES_UNION_PROCESS";
    private static final String EMPLOYEES_TABLE         = "EMPLOYEES_TABLE";
    private static final String US_EMPLOYEES_TABLE      = "US_EMPLOYEES2_TABLE";
    private static final String EMPLOYEES_PROCESS       = "EMPLOYEES_PROCESS";
    private static final String ORDERS_TABLE            = "ORDERS_TABLE";
    private static final String US_ORDERS_TABLE         = "US_ORDERS_TABLE";
    private static final String ORDERS_PROCESS          = "ORDERS_PROCESS";
    private static final String IMPORT_FILE             = "tag-propagation-data.zip";

    private static final String INVALID_CLASSIFICATION_STATUS_CODE        = "ATLAS-400-00-06G";
    private static final String INVALID_CLASSIFICATION_STATUS_UPDATE_CODE = "ATLAS-400-00-06H";

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasEntityStore entityStore;

    @Inject
    private ImportService importService;

    @Inject
    private EntityAuditRepository eventRepository;


    private Map<String, String> entitiesMap;
    private AtlasLineageInfo lineageInfo;

    @BeforeClass
    public void setup() {
        RequestContext.clear();

        loadModelFilesAndImportTestData();
    }

    @AfterClass
    public void clear() throws Exception {
        AtlasGraphProvider.cleanup();

        if (useLocalSolr()) {
            LocalSolrRunner.stop();
        }
    }

    /** This test uses the lineage graph:
     *

     Lineage - 1
     -----------
     [Process1] ----> [Employees1]
     /                              \
     /                                \
     [hdfs_employees]                                  [Process3] ----> [ EmployeesUnion ]
     \                                /
     \                             /
     [Process2] ----> [Employees2]


     Lineage - 2
     -----------

     [Employees] ----> [Process] ----> [ US_Employees ]


     Lineage - 3
     -----------

     [Orders] ----> [Process] ----> [ US_Orders ]

     */

    @Test
    public void testAddClassification() throws AtlasBaseException {
        AtlasEntity         entity = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag2   = new AtlasClassification("tag2");
        tag2.setPropagate(false);

        // add classification without status
        addClassification(entity, tag2);
        entity = getEntity(HDFS_PATH_EMPLOYEES);
        entity.getClassifications().forEach(x -> assertEquals(x.getStatus(), APPROVED));
        assertAuditEntry(entity.getGuid(), CLASSIFICATION_ADD);

        deleteClassification(entity, tag2);

        // add classification with status - APPROVED
        tag2.setStatus(APPROVED);
        addClassification(entity, tag2);
        entity = getEntity(HDFS_PATH_EMPLOYEES);
        entity.getClassifications().forEach(x -> assertEquals(x.getStatus(), APPROVED));

        deleteClassification(entity, tag2);

        // add classification with status - SUGGESTED
        tag2.setStatus(SUGGESTED);
        addClassification(entity, tag2);
        entity = getEntity(HDFS_PATH_EMPLOYEES);
        entity.getClassifications().forEach(x -> assertEquals(x.getStatus(), SUGGESTED));

        deleteClassification(entity, tag2);

        // add classification with status - REJECTED
        tag2.setStatus(REJECTED);
        try {
            addClassification(entity, tag2);
        } catch (AtlasBaseException abe) {
            assertEquals(abe.getAtlasErrorCode().getErrorCode(), INVALID_CLASSIFICATION_STATUS_CODE);
            assertEquals(abe.getMessage(), "Invalid classification status passed for create classification: REJECTED");
        }
        entity = getEntity(HDFS_PATH_EMPLOYEES);
        assertNull(entity.getClassifications());
    }

    @DataProvider(name = "validStatusUpdateProvider")
    private Object[][] validStatusUpdateProvider () {
        return new Object[][]{
                {SUGGESTED, APPROVED},
                {SUGGESTED, REJECTED}
        };
    }

    @Test(dataProvider = "validStatusUpdateProvider")
    public void testUpdateClassificationStatus(AtlasClassification.Status currentStatus, AtlasClassification.Status updatedStatus) throws AtlasBaseException {
        AtlasEntity         entity = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag2   = new AtlasClassification("tag2");
        tag2.setPropagate(false);

        tag2.setStatus(currentStatus);
        addClassification(entity, tag2);

        tag2.setStatus(updatedStatus);
        updateClassification(entity, tag2);
        entity = getEntity(HDFS_PATH_EMPLOYEES);
        entity.getClassifications().forEach(x -> assertEquals(x.getStatus(), updatedStatus));

        deleteClassification(entity, tag2);
    }

    @DataProvider(name = "invalidStatusUpdateProvider")
    private Object[][] invalidStatusUpdateProvider () {
        return new Object[][]{
                {SUGGESTED, SUGGESTED},
                {REJECTED, REJECTED},
                {APPROVED, APPROVED},
                {APPROVED, SUGGESTED},
                {APPROVED, REJECTED},
                {REJECTED, SUGGESTED},
                {REJECTED, APPROVED},
        };
    }

    @Test(dataProvider = "invalidStatusUpdateProvider")
    public void updateClassificationStatusInvalid(AtlasClassification.Status currentStatus, AtlasClassification.Status updatedStatus) throws AtlasBaseException {
        AtlasEntity         entity = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag2   = new AtlasClassification("tag2");

        try {
            deleteClassification(entity, tag2);
        } catch (AtlasBaseException e) {

        }

        tag2.setStatus(SUGGESTED);
        addClassification(entity, tag2);

        tag2.setStatus(currentStatus);
        updateClassification(entity, tag2);

        tag2.setStatus(updatedStatus);
        try {
            updateClassification(entity, tag2);
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode().getErrorCode(), INVALID_CLASSIFICATION_STATUS_UPDATE_CODE);
            assertEquals(e.getMessage(), String.format("Invalid classification status update from %s to %s", currentStatus, updatedStatus));
        }
        deleteClassification(entity, tag2);
    }

    @Test
    public void updateClassificationStatusNull() throws AtlasBaseException {
        AtlasEntity         entity = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag2   = new AtlasClassification("tag2");
        tag2.setPropagate(false);

        tag2.setStatus(SUGGESTED);
        addClassification(entity, tag2);

        // update classification with status as null
        tag2.setStatus(null);
        try {
            updateClassification(entity, tag2);
        } catch (AtlasBaseException e){
            assertEquals(e.getAtlasErrorCode().getErrorCode(), INVALID_CLASSIFICATION_STATUS_CODE);
            assertEquals(e.getMessage(), "Invalid classification status passed for update classification: null");
        }
        deleteClassification(entity, tag2);
    }

    private void loadModelFilesAndImportTestData() {
        try {
            loadModelFromJson("0000-Area0/0010-base_model.json", typeDefStore, typeRegistry);
            loadModelFromJson("1000-Hadoop/1020-fs_model.json", typeDefStore, typeRegistry);
            loadModelFromJson("1000-Hadoop/1030-hive_model.json", typeDefStore, typeRegistry);

            loadSampleClassificationDefs();

            runImportWithNoParameters(importService, getZipSource(IMPORT_FILE));

            initializeEntitiesMap();
        } catch (AtlasBaseException | IOException e) {
            throw new SkipException("Model loading failed!");
        }
    }

    public static InputStream getZipSource(String fileName) throws IOException {
        return ZipFileResourceTestUtils.getFileInputStream(fileName);
    }

    private void loadSampleClassificationDefs() throws AtlasBaseException {
        AtlasClassificationDef tag1 = new AtlasClassificationDef("tag1");
        AtlasClassificationDef tag2 = new AtlasClassificationDef("tag2");
        AtlasClassificationDef tag3 = new AtlasClassificationDef("tag3");
        AtlasClassificationDef tag4 = new AtlasClassificationDef("tag4");

        AtlasClassificationDef PII  = new AtlasClassificationDef("PII");
        PII.addAttribute(new AtlasAttributeDef("type", "string"));
        PII.addAttribute(new AtlasAttributeDef("valid", "boolean"));

        typeDefStore.createTypesDef(new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(),
                                                      Arrays.asList(tag1, tag2, tag3, tag4, PII),
                                                      Collections.emptyList(), Collections.emptyList()));
    }

    private void initializeEntitiesMap() throws AtlasBaseException {
        entitiesMap = new HashMap<>();
        entitiesMap.put(HDFS_PATH_EMPLOYEES, "a3955120-ac17-426f-a4af-972ec8690e5f");
        entitiesMap.put(EMPLOYEES1_TABLE, "cdf0040e-739e-4590-a137-964d10e73573");
        entitiesMap.put(EMPLOYEES2_TABLE, "0a3e66b6-472c-48b3-8453-abdd24f9494f");
        entitiesMap.put(EMPLOYEES_UNION_TABLE, "1ceac963-1a2b-476a-a269-10396187d406");

        entitiesMap.put(EMPLOYEES1_PROCESS, "26dae763-85b7-40af-8516-71056d91d2de");
        entitiesMap.put(EMPLOYEES2_PROCESS, "c0201260-dbeb-45f4-930d-5129eab31dc9");
        entitiesMap.put(EMPLOYEES_UNION_PROCESS, "470a2d1e-b1fd-47de-8f2d-8dfd0a0275a7");

        entitiesMap.put(EMPLOYEES_TABLE, "b4edad46-d00f-4e94-be39-8d2619d17e6c");
        entitiesMap.put(US_EMPLOYEES_TABLE, "44acef8e-fefe-491c-87d9-e2ea6a9ad3b0");
        entitiesMap.put(EMPLOYEES_PROCESS, "a1c9a281-d30b-419c-8199-7434b245d7fe");

        entitiesMap.put(ORDERS_TABLE, "ab995a8d-1f87-4908-91e4-d4e8e376ba22");
        entitiesMap.put(US_ORDERS_TABLE, "70268a81-f145-4a37-ae39-b09daa85a928");
        entitiesMap.put(ORDERS_PROCESS, "da016ad9-456a-4c99-895a-fa00f2de49ba");
    }

    private AtlasEntity getEntity(String entityName) throws AtlasBaseException {
        String                 entityGuid        = entitiesMap.get(entityName);
        AtlasEntityWithExtInfo entityWithExtInfo = entityStore.getById(entityGuid);

        return entityWithExtInfo.getEntity();
    }

    private boolean deleteEntity(String entityName) throws AtlasBaseException {
        String                 entityGuid = entitiesMap.get(entityName);
        EntityMutationResponse response   = entityStore.deleteById(entityGuid);

        return CollectionUtils.isNotEmpty(response.getDeletedEntities());
    }

    private AtlasClassification getClassification(AtlasEntity hdfs_employees, AtlasClassification tag2) throws AtlasBaseException {
        return entityStore.getClassification(hdfs_employees.getGuid(), tag2.getTypeName());
    }

    private void addClassification(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        addClassifications(entity, Collections.singletonList(classification));
    }

    private void addClassifications(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        entityStore.addClassifications(entity.getGuid(), classifications);
    }

    private void updateClassification(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        updateClassifications(entity, Collections.singletonList(classification));
    }

    private void updateClassifications(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        updateClassifications(entity, Collections.singletonList(classification));
    }

    private void updateClassifications(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        entityStore.updateClassifications(entity.getGuid(), classifications);
    }

    private void deleteClassification(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        deleteClassifications(entity, Collections.singletonList(classification.getTypeName()));
    }

    private void deleteClassifications(AtlasEntity entity, List<String> classificationNames) throws AtlasBaseException {
        for (String classificationName : classificationNames) {
            entityStore.deleteClassification(entity.getGuid(), classificationName);
        }
    }

    private void deletePropagatedClassification(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        deletePropagatedClassification(entity, classification.getTypeName(), classification.getEntityGuid());
    }

    private void deletePropagatedClassification(AtlasEntity entity, String classificationName, String associatedEntityGuid) throws AtlasBaseException {
        entityStore.deleteClassification(entity.getGuid(), classificationName, associatedEntityGuid);
    }

    private void deletePropagatedClassificationExpectFail(AtlasEntity entity, AtlasClassification classification) {
        try {
            deletePropagatedClassification(entity, classification);
            fail();
        } catch (AtlasBaseException ex) {
            Assert.equals(ex.getAtlasErrorCode(), PROPAGATED_CLASSIFICATION_REMOVAL_NOT_SUPPORTED);
        }
    }

    private void assertAuditEntry(String guid, EntityAuditEventV2.EntityAuditActionV2 auditAction) throws AtlasBaseException{
        List<EntityAuditEventV2> auditEvents = eventRepository.listEventsV2(guid, auditAction, null, (short) 10);
        assertNotNull(auditEvents);
    }
}