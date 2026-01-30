/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.vertica;

import com.amazonaws.athena.connectors.vertica.query.QueryFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class VerticaSqlUtilsTest {

    @Test
    public void getQueryFactory_ReturnsSameInstance() {
        QueryFactory factory1 = VerticaSqlUtils.getQueryFactory();
        QueryFactory factory2 = VerticaSqlUtils.getQueryFactory();
        assertSame("getQueryFactory should return the same instance", factory1, factory2);
    }

    @Test
    public void renderTemplate_nullPredicateIsNull_ReturnsExpectedString() {
        String result = VerticaSqlUtils.renderTemplate("null_predicate",
                Map.of("columnName", "col1", "isNull", true));
        assertEquals("(col1 IS NULL)", result);
    }

    @Test
    public void renderTemplate_nullPredicateIsNotNull_ReturnsExpectedString() {
        String result = VerticaSqlUtils.renderTemplate("null_predicate",
                Map.of("columnName", "col1", "isNull", false));
        assertEquals("(col1 IS NOT NULL)", result);
    }

    @Test
    public void renderTemplate_comparisonPredicate_ReturnsExpectedString() {
        String result = VerticaSqlUtils.renderTemplate("comparison_predicate",
                Map.of("columnName", "\"col1\"", "operator", "=", "placeholder", "<key>"));
        assertEquals("\"col1\" = <key>", result);
    }

    @Test
    public void renderTemplate_orPredicate_ReturnsExpectedString() {
        String result = VerticaSqlUtils.renderTemplate("or_predicate",
                Map.of("disjuncts", java.util.List.of("(a)", "(b)")));
        assertEquals("((a) OR (b))", result);
    }

    @Test
    public void renderTemplate_rangePredicate_ReturnsExpectedString() {
        String result = VerticaSqlUtils.renderTemplate("range_predicate",
                Map.of("conjuncts", java.util.List.of("\"col\" >= <k1>", "\"col\" \\<= <k2>")));
        assertEquals("(\"col\" >= <k1> AND \"col\" \\<= <k2>)", result);
    }

    @Test
    public void renderTemplate_inPredicate_ReturnsExpectedString() {
        String result = VerticaSqlUtils.renderTemplate("in_predicate",
                Map.of("columnName", "\"col1\"", "placeholders", java.util.List.of("<k1>", "<k2>")));
        assertEquals("\"col1\" IN (<k1>,<k2>)", result);
    }

    @Test
    public void renderTemplate_unknownTemplate_ThrowsRuntimeException() {
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> VerticaSqlUtils.renderTemplate("nonexistent_template", Collections.emptyMap()));
        assertEquals("Template not found: nonexistent_template", ex.getMessage());
    }

    @Test
    public void renderTemplate_emptyParams_ReturnsRenderedTemplate() {
        String result = VerticaSqlUtils.renderTemplate("null_predicate",
                Map.of("columnName", "x", "isNull", true));
        assertEquals("(x IS NULL)", result);
    }
}
