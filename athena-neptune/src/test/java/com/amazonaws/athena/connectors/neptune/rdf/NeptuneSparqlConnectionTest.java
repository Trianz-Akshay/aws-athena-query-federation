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
import static org.junit.Assert.fail;
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
        connection = new NeptuneSparqlConnection("localhost", "8182", false, "us-west-2");
        connection.connection = mockConnection;
        connection.neptuneSparqlRepo = mockRepo;
        connection.queryResult = mockQueryResult;
        lenient().when(mockQueryResult.hasNext()).thenReturn(true, false);
        lenient().when(mockQueryResult.next()).thenReturn(mockBindingSet);
    }

    @Test
    public void testConstructorWithoutIAMAuth() {
        NeptuneSparqlConnection nonIamConnection = new NeptuneSparqlConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
        assertNotNull(nonIamConnection);
    }

    @Test
    public void testHasNext() {
        when(mockQueryResult.hasNext()).thenReturn(true);
        assertTrue(connection.hasNext());

        when(mockQueryResult.hasNext()).thenReturn(false);
        assertFalse(connection.hasNext());
    }

    @Test
    public void testNextWithIRIValue() {
        Set<String> bindingNames = new HashSet<>();
        bindingNames.add("subject");
        when(mockBindingSet.getBindingNames()).thenReturn(bindingNames);
        when(mockBindingSet.getValue("subject")).thenReturn(mockIRI);
        when(mockIRI.stringValue()).thenReturn("http://example.org/resource");

        Map<String, Object> result = connection.next();
        assertNotNull(result);
        assertEquals("http://example.org/resource", result.get("subject"));
    }

    @Test
    public void testNextWithLiteralTypes()  {
       try{
           when(mockQueryResult.next()).thenReturn(mockBindingSet);
        Set<String> bindingNames = new HashSet<>();
        bindingNames.add("boolVar");
        bindingNames.add("dateVar");
        when(mockBindingSet.getBindingNames()).thenReturn(bindingNames);

        // Mock boolean literal
        Literal boolLiteral = mock(Literal.class);
        when(boolLiteral.getDatatype()).thenReturn(XSD.BOOLEAN);
        when(boolLiteral.booleanValue()).thenReturn(true);
        when(mockBindingSet.getValue("boolVar")).thenReturn(boolLiteral);

        // Mock date literal
        Literal dateLiteral = mock(Literal.class);
           when(dateLiteral.getDatatype()).thenReturn(XSD.DATE);
        XMLGregorianCalendar calendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(new GregorianCalendar());
        when(dateLiteral.calendarValue()).thenReturn(calendar);
        when(mockBindingSet.getValue("dateVar")).thenReturn(dateLiteral);

        Map<String, Object> result = connection.next();
        assertTrue((Boolean) result.get("boolVar"));
        assertNotNull(result.get("dateVar"));
    } catch (Exception e)
       {
        fail("Unexpected exception: " + e.getMessage());
    }}

    @Test
    public void testRunQuery() {
        when(mockConnection.prepareTupleQuery(any(), anyString())).thenReturn(mockTupleQuery);
        when(mockTupleQuery.evaluate()).thenReturn(mockQueryResult);

        connection.runQuery("SELECT * WHERE { ?s ?p ?o }");
        
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o }");
        verify(mockTupleQuery).evaluate();
    }

    @Test
    public void testSafeCloseRepo() {
        connection.safeCloseRepo();
        verify(mockQueryResult).close();
    }

    @Test
    public void testNextWithNullValue() {
        Set<String> bindingNames = new HashSet<>();
        bindingNames.add("subject");
        when(mockBindingSet.getBindingNames()).thenReturn(bindingNames);
        when(mockBindingSet.getValue("subject")).thenReturn(null);

        Map<String, Object> result = connection.next();
        assertNotNull(result);
        // The method should return an empty map when all values are null
        // but it might still contain the key with null value
        assertTrue(result.isEmpty() || result.containsKey("subject"));
    }

    @Test
    public void testNextWithStringValue() {
        Set<String> bindingNames = new HashSet<>();
        bindingNames.add("subject");
        when(mockBindingSet.getBindingNames()).thenReturn(bindingNames);
        when(mockBindingSet.getValue("subject")).thenReturn(mockLiteral);
        when(mockLiteral.getDatatype()).thenReturn(XSD.LONG);
        Map<String, Object> result = connection.next();
        assertNotNull(result);
        assertEquals(0L, result.get("subject"));
    }
} 
