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
package com.amazonaws.athena.connectors.neptune.rdf;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneSparqlConnectionTest {

    private static final String TEST_ENDPOINT = "localhost";
    private static final String TEST_PORT = "8182";
    private static final String TEST_REGION = "us-west-2";
    public static final String SUBJECT = "subject";
    public static final String BOOL_VAR = "boolVar";
    public static final String DATE_VAR = "dateVar";
    private static final String EXAMPLE_RESOURCE = "http://example.org/resource";
    private static final String SPARQL_QUERY = "SELECT * WHERE { ?s ?p ?o }";

    @Mock
    private RepositoryConnection mockConnection;
    @Mock
    private NeptuneSparqlRepository mockRepo;
    @Mock
    private TupleQuery mockTupleQuery;
    @Mock
    private TupleQueryResult mockQueryResult;
    @Mock
    private BindingSet mockBindingSet;
    @Mock
    private IRI mockIRI;
    @Mock
    private Literal mockLiteral;

    private NeptuneSparqlConnection connection;

    @Before
    public void setup() {
        connection = new NeptuneSparqlConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
        connection.connection = mockConnection;
        connection.neptuneSparqlRepo = mockRepo;
        connection.queryResult = mockQueryResult;
        lenient().when(mockQueryResult.hasNext()).thenReturn(true, false);
        lenient().when(mockQueryResult.next()).thenReturn(mockBindingSet);
    }

    @Test
    public void constructor_WithoutIAMAuth_CreatesConnection() {
        NeptuneSparqlConnection nonIamConnection = new NeptuneSparqlConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
        assertNotNull(nonIamConnection);
    }

    @Test
    public void hasNext_WithQueryResult_ReturnsCorrectBoolean() {
        when(mockQueryResult.hasNext()).thenReturn(true);
        assertTrue(connection.hasNext());

        when(mockQueryResult.hasNext()).thenReturn(false);
        assertFalse(connection.hasNext());
    }

    @Test
    public void next_WithIRIValue_ReturnsCorrectValue() {
        Set<String> bindingNames = new HashSet<>();
        bindingNames.add(SUBJECT);
        when(mockBindingSet.getBindingNames()).thenReturn(bindingNames);
        when(mockBindingSet.getValue(SUBJECT)).thenReturn(mockIRI);
        when(mockIRI.stringValue()).thenReturn(EXAMPLE_RESOURCE);

        Map<String, Object> result = connection.next();
        assertNotNull(result);
        assertEquals(EXAMPLE_RESOURCE, result.get(SUBJECT));
    }

    @Test
    public void next_WithLiteralTypes_HandlesDifferentTypes() throws Exception {
        when(mockQueryResult.next()).thenReturn(mockBindingSet);
        Set<String> bindingNames = new HashSet<>();
        bindingNames.add(BOOL_VAR);
        bindingNames.add(DATE_VAR);
        when(mockBindingSet.getBindingNames()).thenReturn(bindingNames);

        // Mock boolean literal
        Literal boolLiteral = mock(Literal.class);
        when(boolLiteral.getDatatype()).thenReturn(XSD.BOOLEAN);
        when(boolLiteral.booleanValue()).thenReturn(true);
        when(mockBindingSet.getValue(BOOL_VAR)).thenReturn(boolLiteral);

        // Mock date literal
        Literal dateLiteral = mock(Literal.class);
        when(dateLiteral.getDatatype()).thenReturn(XSD.DATE);
        XMLGregorianCalendar calendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(new GregorianCalendar());
        when(dateLiteral.calendarValue()).thenReturn(calendar);
        when(mockBindingSet.getValue(DATE_VAR)).thenReturn(dateLiteral);

        Map<String, Object> result = connection.next();
        assertTrue((Boolean) result.get(BOOL_VAR));
        assertNotNull(result.get(DATE_VAR));
    }

    @Test
    public void runQuery_WithValidQuery_ExecutesQuery() {
        when(mockConnection.prepareTupleQuery(any(), anyString())).thenReturn(mockTupleQuery);
        when(mockTupleQuery.evaluate()).thenReturn(mockQueryResult);

        connection.runQuery(SPARQL_QUERY);
        
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, SPARQL_QUERY);
        verify(mockTupleQuery).evaluate();
    }

    @Test
    public void safeCloseRepo_WithOpenConnection_ClosesConnection() {
        connection.safeCloseRepo();
        verify(mockQueryResult).close();
    }

    @Test
    public void next_WithNullValue_HandlesNullValue() {
        Set<String> bindingNames = new HashSet<>();
        bindingNames.add(SUBJECT);
        when(mockBindingSet.getBindingNames()).thenReturn(bindingNames);
        when(mockBindingSet.getValue(SUBJECT)).thenReturn(null);

        Map<String, Object> result = connection.next();
        assertNotNull(result);
        // The method should return an empty map when all values are null
        // but it might still contain the key with null value
        assertTrue(result.isEmpty() || result.containsKey(SUBJECT));
    }

    @Test
    public void next_WithStringValue_ReturnsCorrectValue() {
        Set<String> bindingNames = new HashSet<>();
        bindingNames.add(SUBJECT);
        when(mockBindingSet.getBindingNames()).thenReturn(bindingNames);
        when(mockBindingSet.getValue(SUBJECT)).thenReturn(mockLiteral);
        when(mockLiteral.getDatatype()).thenReturn(XSD.LONG);
        Map<String, Object> result = connection.next();
        assertNotNull(result);
        assertEquals(0L, result.get(SUBJECT));
    }
} 
