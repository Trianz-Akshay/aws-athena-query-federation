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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneEnvironmentPropertiesTest {
    private static final String TEST_ENDPOINT = "localhost";
    private static final String TEST_PORT = "8182";
    private static final String TEST_REGION = "us-east-1";
    private static final String TEST_IAM = "false";
    private static final String TEST_GRAPH_TYPE = "PROPERTYGRAPH";

    @Mock
    private Map<String, String> mockEnv;

    @Before
    public void setUp() {
        mockEnv = new HashMap<>();
        mockEnv.put(Constants.CFG_ENDPOINT, TEST_ENDPOINT);
        mockEnv.put(Constants.CFG_PORT, TEST_PORT);
        mockEnv.put(Constants.CFG_GRAPH_TYPE, TEST_GRAPH_TYPE);
        mockEnv.put(Constants.CFG_IAM, TEST_IAM);
        mockEnv.put(Constants.CFG_REGION, TEST_REGION);
    }

    @Test
    public void createEnvironment_WithValidConfiguration_ReturnsNonNullEnvironmentValues() {
        NeptuneEnvironmentProperties properties = new NeptuneEnvironmentProperties() {
            @Override
            public Map<String, String> createEnvironment() {
                return mockEnv;
            }
        };

        Map<String, String> env = properties.createEnvironment();
        assertNotNull(env);
        assertNotNull(env.get(Constants.CFG_ENDPOINT));
        assertNotNull(env.get(Constants.CFG_PORT));
        assertNotNull(env.get(Constants.CFG_GRAPH_TYPE));
        assertNotNull(env.get(Constants.CFG_IAM));
        assertNotNull(env.get(Constants.CFG_REGION));
    }

    @Test
    public void createEnvironment_WithValidConfiguration_ReturnsMatchingEnvironmentValues() {
        NeptuneEnvironmentProperties properties = new NeptuneEnvironmentProperties() {
            @Override
            public Map<String, String> createEnvironment() {
                return mockEnv;
            }
        };

        Map<String, String> env = properties.createEnvironment();
        assertEquals(TEST_ENDPOINT, env.get(Constants.CFG_ENDPOINT));
        assertEquals(TEST_PORT, env.get(Constants.CFG_PORT));
        assertEquals(TEST_GRAPH_TYPE, env.get(Constants.CFG_GRAPH_TYPE));
        assertEquals(TEST_IAM, env.get(Constants.CFG_IAM));
        assertEquals(TEST_REGION, env.get(Constants.CFG_REGION));
    }

    @Test
    public void createEnvironment_WithValidConfiguration_ReturnsExpectedEnvironmentSize() {
        NeptuneEnvironmentProperties properties = new NeptuneEnvironmentProperties() {
            @Override
            public Map<String, String> createEnvironment() {
                return mockEnv;
            }
        };

        Map<String, String> env = properties.createEnvironment();
        assertEquals(5, env.size());
    }
} 