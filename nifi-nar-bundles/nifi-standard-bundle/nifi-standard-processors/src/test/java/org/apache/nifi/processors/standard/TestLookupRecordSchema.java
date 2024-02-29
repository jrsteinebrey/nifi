/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.standard;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLookupRecordSchema {

    private TestRunner runner;
    private MapLookup lookupService;
    private MockRecordParser recordReader;
    private MockRecordWriter recordWriter;

    @BeforeEach
    public void setup() throws InitializationException {
        recordReader = new MockRecordParser();
        recordWriter = new MockRecordWriter(null, false);
        lookupService = new MapLookup();

        runner = TestRunners.newTestRunner(LookupRecord.class);
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.addControllerService("lookup", lookupService);
        runner.enableControllerService(lookupService);

        runner.setProperty(LookupRecord.RECORD_READER, "reader");
        runner.setProperty(LookupRecord.RECORD_WRITER, "writer");
        runner.setProperty(LookupRecord.LOOKUP_SERVICE, "lookup");
        runner.setProperty("lookup", "/name");
        runner.setProperty(LookupRecord.RESULT_RECORD_PATH, "/sport");
        runner.setProperty(LookupRecord.ROUTING_STRATEGY, LookupRecord.ROUTE_TO_MATCHED_UNMATCHED);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, null, null);
        recordReader.addRecord("Jane Doe", 47, null, null);
        recordReader.addRecord("Jimmy Doe", 14, null, null);
    }

    /**
     * If the output fields are added to a non-record field, then the result should be that the field
     * becomes a UNION that does allow the Record and the value is set to a Record.
     */
    private TestRunner createRunnerAndInitialize(JsonTreeReader jsonReader,
                                                 String readerSchemaText,
                                                 JsonRecordSetWriter jsonWriter,
                                                 AllowableValue writerSuppressNulls,
                                                 MapLookup lookupService) throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(LookupRecord.class);
        runner.addControllerService("reader", jsonReader);

        if (StringUtils.isBlank(readerSchemaText)) {
            runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        } else {
            //final RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
            //        new RecordField("foo", RecordFieldType.STRING.getDataType()),
            //        new RecordField("bar", RecordFieldType.STRING.getDataType())
            //));
            //String schemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestConversions/explicit.schema.json")));
            runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
            runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, readerSchemaText);
        }

        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(jsonWriter, JsonRecordSetWriter.SUPPRESS_NULLS, writerSuppressNulls);

        runner.enableControllerService(jsonReader);
        runner.enableControllerService(jsonWriter);
        runner.addControllerService("lookup", lookupService);
        runner.enableControllerService(lookupService);

        runner.setProperty(LookupRecord.ROUTING_STRATEGY, LookupRecord.ROUTE_TO_MATCHED_UNMATCHED);
        runner.setProperty(LookupRecord.REPLACEMENT_STRATEGY, LookupRecord.REPLACE_EXISTING_VALUES);
        runner.setProperty(LookupRecord.RECORD_READER, "reader");
        runner.setProperty(LookupRecord.RECORD_WRITER, "writer");
        runner.setProperty(LookupRecord.LOOKUP_SERVICE, "lookup");

        return runner;
    }

    @Test
    public void testLookupMissingJsonFieldNotNestedInferredWorks() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        final MapLookup lookupService = new MapLookupForInPlaceReplacement();
        TestRunner runner = createRunnerAndInitialize(
                jsonReader, null,
                jsonWriter, JsonRecordSetWriter.NEVER_SUPPRESS,
                lookupService);

        runner.setProperty("lookupFoo", "/Int_Float_String");
        lookupService.addValue("original", "updated");

        //runner.enqueue("[{ \"foo\" : \"original\", \"bar\" : \"unchanged\"},  {\"bar\" : \"original\" }]");
        runner.enqueue("[{ \"Id\" : 1, \"Int_Float_String\" : \"original\"},  {\"Int_Float_String\" : \"original\" }]");
        runner.run();

        //runner.assertTransferCount(LookupRecord.REL_UNMATCHED, 1);
        //final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);
        //String unmatched = outUnmatched.getContent();

        runner.assertTransferCount(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);
        String matched = outMatched.getContent();

        outMatched.assertContentEquals("[{\"Id\":1,\"Int_Float_String\":\"updated\"},{\"Id\":null,\"Int_Float_String\":\"updated\"}]");
    }

    @Test
    public void testLookupMissingJsonFieldNotNestedExplicitWorks() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        String schemaText = new String(Files.readAllBytes(
                Paths.get("src/test/resources/TestConversions/explicit.schema.json")));
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        final MapLookup lookupService = new MapLookupForInPlaceReplacement();
        TestRunner runner = createRunnerAndInitialize(
                jsonReader, schemaText,
                jsonWriter, JsonRecordSetWriter.NEVER_SUPPRESS,
                lookupService);

        runner.setProperty("lookupFoo", "/Int_Float_String");
        lookupService.addValue("original", "updated");

        //runner.enqueue("[{ \"foo\" : \"original\", \"bar\" : \"unchanged\"},  {\"bar\" : \"original\" }]");
        runner.enqueue("[{ \"Id\" : 1, \"Int_Float_String\" : \"original\"},  {\"Int_Float_String\" : \"original\" }]");
        runner.run();

        //runner.assertTransferCount(LookupRecord.REL_UNMATCHED, 1);
        //final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);
        //String unmatched = outUnmatched.getContent();

        runner.assertTransferCount(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);
        String matched = outMatched.getContent();

        outMatched.assertContentEquals("[{\"Id\":1,\"Int_Float_String\":\"updated\"},{\"Id\":null,\"Int_Float_String\":\"updated\"}]");
    }

    @Test
    public void testLookupMissingJsonFieldNestedInferredFails() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        String schemaText = null;
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        final MapLookup lookupService = new MapLookupForInPlaceReplacement();
        TestRunner runner = createRunnerAndInitialize(
                jsonReader, schemaText,
                jsonWriter, JsonRecordSetWriter.NEVER_SUPPRESS,
                lookupService);

        //runner.setProperty("lookupFoo", "/Int_Float_String");
        runner.setProperty("lookupChild", "/Id/child");

        lookupService.addValue("original", "updated");
        lookupService.addValue("1", "2");

        //runner.enqueue("[{ \"foo\" : \"original\", \"bar\" : \"unchanged\"},  {\"bar\" : \"original\" }]");
        runner.enqueue("[{ \"Id\" :  {  \"child\" : \"1\" }, \"Int_Float_String\" : \"original\"},  {\"Int_Float_String\" : \"original\" }]");
        runner.run();

        runner.assertTransferCount(LookupRecord.REL_UNMATCHED, 1);
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);
        String unmatched = outUnmatched.getContent();

        runner.assertTransferCount(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);
        String matched = outMatched.getContent();

        outMatched.assertContentEquals("[{\"Id\":{\"child\":\"2\"},\"Int_Float_String\":\"original\"}]");
        outUnmatched.assertContentEquals("[{\"Id\":null,\"Int_Float_String\":\"original\"}]");
    }

    @Test
    public void testLookupMissingJsonFieldNestedInferredWorks() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        String schemaText = null;
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        final MapLookup lookupService = new MapLookupForInPlaceReplacement();
        TestRunner runner = createRunnerAndInitialize(
                jsonReader, schemaText,
                jsonWriter, JsonRecordSetWriter.NEVER_SUPPRESS,
                lookupService);

        //runner.setProperty("lookupFoo", "/Int_Float_String");
        runner.setProperty("lookupIntFloatString", "/Int_Float_String");
        lookupService.addValue("original", "updated");
        //lookupService.addValue("1", "2");

        //runner.enqueue("[{ \"foo\" : \"original\", \"bar\" : \"unchanged\"},  {\"bar\" : \"original\" }]");
        runner.enqueue("[{ \"Id\" : { \"child\" : \"1\" }, \"Int_Float_String\" : \"original\"}, {\"Int_Float_String\" : \"original\" }]");
        runner.run();

        //runner.assertTransferCount(LookupRecord.REL_UNMATCHED, 1);
        //final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);
        //String unmatched = outUnmatched.getContent();

        runner.assertTransferCount(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);
        String matched = outMatched.getContent();

        outMatched.assertContentEquals("[{\"Id\":{\"child\":\"1\"},\"Int_Float_String\":\"updated\"},{\"Id\":null,\"Int_Float_String\":\"updated\"}]");
    }

    @Test
    public void testLookupMissingJsonFieldExplicitNested() throws InitializationException, IOException {

        String schemaText = """
{
  "type": "record",
  "name": "Record",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "Id",
      "type": [
        {
          "type": "record",
          "name": "O",
          "namespace": "org.apache.nifi",
          "fields": [
            {
              "name": "child",
              "type": [
                "string",
                "null"
              ]
            }
          ]
        },
        {
          "name": "Int_Float_String",
          "type": "string"
        }
      ]
    }
  ]
}
""";
        final JsonTreeReader jsonReader = new JsonTreeReader();
        //String schemaText = null;
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        final MapLookup lookupService = new MapLookupForInPlaceReplacement();
        TestRunner runner = createRunnerAndInitialize(
                jsonReader, schemaText,
                jsonWriter, JsonRecordSetWriter.NEVER_SUPPRESS,
                lookupService);

        runner.setProperty("lookupFoo", "/Int_Float_String");

        lookupService.addValue("original", "updated");

        //runner.enqueue("[{ \"foo\" : \"original\", \"bar\" : \"unchanged\"},  {\"bar\" : \"original\" }]");
        runner.enqueue("[{ \"Id\" : { \"child\" : \"1\" }, \"Int_Float_String\" : \"original\"},  {\"Int_Float_String\" : \"original\" }]");
        runner.run();

        runner.assertTransferCount(LookupRecord.REL_UNMATCHED, 1);
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);
        String unmatched = outUnmatched.getContent();

        runner.assertTransferCount(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);
        String matched = outMatched.getContent();

        //outMatched.assertContentEquals("[{\"Id\":1,\"Int_Float_String\":\"updated\"},{\"Id\":null,\"Int_Float_String\":\"updated\"}]");
        outMatched.assertContentEquals("[{\"Id\":{\"child\":\"1\"},\"Int_Float_String\":\"updated\"},{\"Id\":null,\"Int_Float_String\":\"updated\"}]");
    }

    private static class MapLookup extends AbstractControllerService implements StringLookupService {
        protected final Map<String, String> values = new HashMap<>();
        private Map<String, Object> expectedContext;

        public void addValue(final String key, final String value) {
            values.put(key, value);
        }

        @Override
        public Class<?> getValueType() {
            return String.class;
        }

        @Override
        public Optional<String> lookup(final Map<String, Object> coordinates, Map<String, String> context) {
            validateContext(context);
            return lookup(coordinates);
        }

        @Override
        public Optional<String> lookup(final Map<String, Object> coordinates) {
            if (coordinates == null || coordinates.get("lookup") == null) {
                return Optional.empty();
            }

            final String key = coordinates.containsKey("lookup") ? coordinates.get("lookup").toString() : null;
            if (key == null) {
                return Optional.empty();
            }

            return Optional.ofNullable(values.get(key));
        }

        @Override
        public Set<String> getRequiredKeys() {
            return Collections.singleton("lookup");
        }

        public void setExpectedContext(Map<String, Object> expectedContext) {
            this.expectedContext = expectedContext;
        }

        private void validateContext(Map<String, String> context) {
            if (expectedContext != null) {
                for (Map.Entry<String, Object> entry : expectedContext.entrySet()) {
                    assertTrue(context.containsKey(entry.getKey()),
                            String.format("%s was not in coordinates.", entry.getKey()));
                    assertEquals(entry.getValue(), context.get(entry.getKey()), "Wrong value");
                }
            }
        }
    }

    private static class RecordLookup extends AbstractControllerService implements RecordLookupService {
        private final Map<String, Record> values = new HashMap<>();

        public void addValue(final String key, final Record value) {
            values.put(key, value);
        }

        @Override
        public Class<?> getValueType() {
            return String.class;
        }

        @Override
        public Optional<Record> lookup(final Map<String, Object> coordinates) {
            if (coordinates == null || coordinates.get("lookup") == null) {
                return Optional.empty();
            }

            final String key = (String)coordinates.get("lookup");
            if (key == null) {
                return Optional.empty();
            }

            return Optional.ofNullable(values.get(key));
        }

        @Override
        public Set<String> getRequiredKeys() {
            return Collections.singleton("lookup");
        }
    }

    private static class MapLookupForInPlaceReplacement extends TestLookupRecordSchema.MapLookup implements StringLookupService {
        @Override
        public Optional<String> lookup(final Map<String, Object> coordinates) {
            final String key = (String)coordinates.values().iterator().next();
            if (key == null) {
                return Optional.empty();
            }

            return Optional.ofNullable(values.get(key));
        }
    }
}
