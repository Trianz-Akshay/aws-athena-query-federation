/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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

import static org.junit.Assert.assertEquals;

public class EnumsTest {

    @Test
    public void graphTypeValues_ValidEnumValues_ReturnsCorrectEnumInstances() {
        assertEquals(2, Enums.GraphType.values().length);
        assertEquals(Enums.GraphType.PROPERTYGRAPH, Enums.GraphType.valueOf("PROPERTYGRAPH"));
        assertEquals(Enums.GraphType.RDF, Enums.GraphType.valueOf("RDF"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void graphTypeValueOf_InvalidEnumValue_ThrowsIllegalArgumentException() {
        Enums.GraphType.valueOf("INVALID");
    }

    @Test
    public void tableSchemaMetaTypeValues_ValidEnumValues_ReturnsCorrectEnumInstances() {
        assertEquals(3, Enums.TableSchemaMetaType.values().length);
        assertEquals(Enums.TableSchemaMetaType.VERTEX, Enums.TableSchemaMetaType.valueOf("VERTEX"));
        assertEquals(Enums.TableSchemaMetaType.EDGE, Enums.TableSchemaMetaType.valueOf("EDGE"));
        assertEquals(Enums.TableSchemaMetaType.VIEW, Enums.TableSchemaMetaType.valueOf("VIEW"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void tableSchemaMetaTypeValueOf_InvalidEnumValue_ThrowsIllegalArgumentException() {
        Enums.TableSchemaMetaType.valueOf("INVALID");
    }
} 