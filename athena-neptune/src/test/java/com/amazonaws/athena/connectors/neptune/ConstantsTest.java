/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.neptune;

import org.junit.Test;
import static org.junit.Assert.*;

public class ConstantsTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testConstructorThrowsException() {
        new Constants();
    }

    @Test
    public void testConstantValues() {
        assertEquals("neptune", Constants.SOURCE_TYPE);
        assertEquals("neptune_graphtype", Constants.CFG_GRAPH_TYPE);
        assertEquals("neptune_endpoint", Constants.CFG_ENDPOINT);
        assertEquals("neptune_port", Constants.CFG_PORT);
        assertEquals("iam_enabled", Constants.CFG_IAM);
        assertEquals("AWS_REGION", Constants.CFG_REGION);
        assertEquals("neptune_cluster_res_id", Constants.CFG_ClUSTER_RES_ID);
        assertEquals("query", Constants.SCHEMA_QUERY);
        assertEquals("enable_caseinsensitivematch", Constants.SCHEMA_CASE_INSEN);
        assertEquals("componenttype", Constants.SCHEMA_COMPONENT_TYPE);
        assertEquals("glabel", Constants.SCHEMA_GLABEL);
        assertEquals("strip_uri", Constants.SCHEMA_STRIP_URI);
        assertEquals("prefix_prop", Constants.SCHEMA_PREFIX_PROP);
        assertEquals("prefix_class", Constants.SCHEMA_PREFIX_CLASS);
        assertEquals("querymode", Constants.SCHEMA_QUERY_MODE);
        assertEquals("classuri", Constants.SCHEMA_CLASS_URI);
        assertEquals("subject", Constants.SCHEMA_SUBJECT);
    }
} 