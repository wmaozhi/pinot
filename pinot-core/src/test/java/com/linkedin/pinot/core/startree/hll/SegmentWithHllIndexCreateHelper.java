/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree.hll;

import com.linkedin.pinot.common.data.*;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

public class SegmentWithHllIndexCreateHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentWithHllIndexCreateHelper.class);

    private File INDEX_DIR;
    private File inputAvro;
    private String timeColumnName;
    private TimeUnit timeUnit;
    private Schema schema;
    private static final String hllDeriveColumnSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;

    public SegmentWithHllIndexCreateHelper(String tempDirName, String avroDataPath,
                                           String timeColumnName, TimeUnit timeUnit) throws IOException {
        INDEX_DIR = Files.createTempDirectory(SegmentWithHllIndexCreateHelper.class.getName() + "_" + tempDirName).toFile();
        LOGGER.info("INDEX_DIR: {}", INDEX_DIR.getAbsolutePath());
        String filePath = TestUtils.getFileFromResourceUrl(
                SegmentV1V2ToV3FormatConverter.class.getClassLoader().getResource(avroDataPath));
        inputAvro = new File(filePath);
        LOGGER.info("Input Avro: {}", inputAvro.getAbsolutePath());
        this.timeColumnName = timeColumnName;
        this.timeUnit = timeUnit;
    }

    /**
     * must call this to clean up
     */
    public void cleanTempDir() {
        if (INDEX_DIR != null) {
            FileUtils.deleteQuietly(INDEX_DIR);
        }
    }

    private static void printSchema(Schema schema) {
        LOGGER.info("schemaName: {}", schema.getSchemaName());
        LOGGER.info("Dimension columnNames: ");
        int i = 0;
        for (DimensionFieldSpec spec: schema.getDimensionFieldSpecs()) {
            String columnInfo = i + " " + spec.getName();
            if (!spec.isSingleValueField()) {
                LOGGER.info(columnInfo + " Multi-Value.");
            } else {
                LOGGER.info(columnInfo);
            }
            i += 1;
        }
        LOGGER.info("Metric columnNames: ");
        i = 0;
        for (MetricFieldSpec spec: schema.getMetricFieldSpecs()) {
            String columnInfo = i + " " + spec.getName();
            if (!spec.isSingleValueField()) {
                LOGGER.info(columnInfo + " Multi-Value.");
            } else {
                LOGGER.info(columnInfo);
            }
            i += 1;
        }
        LOGGER.info("Time column: {}", schema.getTimeColumnName());
    }

    private void addTimeColumnToSchema(SegmentGeneratorConfig segmentGenConfig) throws Exception {
        // set time column in schema
        DataFileStream<GenericRecord> dataStream =
                new DataFileStream<>(new FileInputStream(inputAvro), new GenericDatumReader<GenericRecord>());
        final TimeGranularitySpec gSpec = new TimeGranularitySpec(
                 /* time column data type*/
                SegmentTestUtils.getColumnType(dataStream.getSchema().getField(timeColumnName)),
                timeUnit,
                timeColumnName);
        final TimeFieldSpec fSpec = new TimeFieldSpec(gSpec);
        segmentGenConfig.getSchema().addField(timeColumnName, fSpec);
        dataStream.close();
    }

    private void setupStarTreeConfig(SegmentGeneratorConfig segmentGenConfig) {
        // StarTree related
        segmentGenConfig.setEnableStarTreeIndex(true);
        StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();
        starTreeIndexSpec.setMaxLeafRecords(StarTreeIndexSpec.DEFAULT_MAX_LEAF_RECORDS);
        segmentGenConfig.setStarTreeIndexSpec(starTreeIndexSpec);
        LOGGER.info("segmentGenConfig Schema (w/o derived fields): ");
        printSchema(segmentGenConfig.getSchema());
    }

    public SegmentIndexCreationDriver build(boolean enableStarTree, HllConfig hllConfig) throws Exception {
        final SegmentGeneratorConfig segmentGenConfig = new SegmentGeneratorConfig(
                SegmentTestUtils.extractSchemaFromAvroWithoutTime(inputAvro));

        addTimeColumnToSchema(segmentGenConfig);

        // set other fields in segmentGenConfig
        segmentGenConfig.setInputFilePath(inputAvro.getAbsolutePath());
        segmentGenConfig.setTimeColumnName(timeColumnName);
        segmentGenConfig.setSegmentTimeUnit(timeUnit);
        segmentGenConfig.setFormat(FileFormat.AVRO);
        segmentGenConfig.setSegmentVersion(SegmentVersion.v1);
        segmentGenConfig.setTableName("testTable");
        segmentGenConfig.setOutDir(INDEX_DIR.getAbsolutePath());
        segmentGenConfig.createInvertedIndexForAllColumns();
        segmentGenConfig.setSegmentName("starTreeSegment");
        segmentGenConfig.setSegmentNamePostfix("1");

        if (enableStarTree) {
            setupStarTreeConfig(segmentGenConfig);
            segmentGenConfig.setHllConfig(hllConfig);
        }

        final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
        driver.init(segmentGenConfig);
        /**
         * derived field (hll) is added during the segment build process
         *
         * {@link SegmentIndexCreationDriverImpl#buildStarTree}
         * {@link SegmentIndexCreationDriverImpl#augmentSchemaWithDerivedColumns}
         * {@link SegmentIndexCreationDriverImpl#populateDefaultDerivedColumnValues}
         */
        driver.build();

        LOGGER.info("segmentGenConfig Schema (w/ derived fields): ");
        schema = segmentGenConfig.getSchema();
        printSchema(schema);

        return driver;
    }

    public Schema getSchema() {
        if (schema == null) {
            throw new RuntimeException("Call build first to get schema.");
        }
        return schema;
    }

    public File getIndexDir() {
        return INDEX_DIR;
    }
}