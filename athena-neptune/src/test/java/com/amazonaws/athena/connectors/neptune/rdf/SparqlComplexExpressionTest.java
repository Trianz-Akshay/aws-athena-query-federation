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

import com.amazonaws.athena.connectors.neptune.Constants;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparqlComplexExpressionTest
{
    private static final String LOCALHOST = "localhost";
    private static final String PORT = "8182";
    private static final String REGION = "us-west-2";
    private static final String MULTIPLE_CONDITIONS_QUERY = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . FILTER (?age > 25 && ?salary < 100000) }";
    private static final String NESTED_CONDITIONS_QUERY = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . FILTER ((?age >= 18 && ?age <= 65) || ?status = 'admin') }";
    private static final String ORDER_BY_QUERY = "SELECT ?s ?name ?age WHERE { ?s ?name ?age . FILTER(?age > 25) } ORDER BY ?name ASC, ?age DESC";
    private static final String LIMIT_QUERY = "SELECT ?s ?name ?age WHERE { ?s ?name ?age . FILTER(?age > 25) } ORDER BY ?name ASC LIMIT 10";
    private static final String DEFAULT_QUERY = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
    private static final String TOP_N_QUERY = "SELECT ?s ?name ?salary WHERE { ?s ?name ?salary . FILTER(?salary > 50000) } ORDER BY ?salary DESC LIMIT 5";
    private static final String MIXED_DATA_TYPES_QUERY = "SELECT ?s ?name ?age ?active WHERE { ?s ?name ?age ?active . FILTER(?name = 'John' && ?age > 25 && ?active = true) } ORDER BY ?name ASC, ?age DESC";
    private static final String NOT_EQUAL_QUERY = "SELECT ?s ?status ?role WHERE { ?s ?status ?role . FILTER(?status != 'inactive' && ?role != 'guest') } ORDER BY ?status ASC";
    private static final String LONG_VALUES_QUERY = "SELECT ?s ?timestamp ?id WHERE { ?s ?timestamp ?id . FILTER(?timestamp > 1609459200000 && ?id != 0) } ORDER BY ?id ASC, ?timestamp DESC";
    private static final String BOOLEAN_LOGIC_QUERY = "SELECT ?s ?isActive ?isVerified WHERE { ?s ?isActive ?isVerified . FILTER(?isActive = true && ?isVerified = false) } ORDER BY ?isActive ASC, ?isVerified DESC";
    private static final String FLOAT_COMPARISONS_QUERY = "SELECT ?s ?price ?rating WHERE { ?s ?price ?rating . FILTER(?price >= 10.5 && ?rating <= 4.8) } ORDER BY ?price ASC, ?rating DESC";
    private static final String MULTIPLE_ORDER_BY_QUERY = "SELECT ?s ?department ?salary ?name WHERE { ?s ?department ?salary ?name . FILTER(?salary > 50000) } ORDER BY ?department ASC, ?salary DESC, ?name ASC";
    
    @Mock
    private RepositoryConnection mockConnection;
    @Mock
    private TupleQuery mockTupleQuery;
    @Mock
    private TupleQueryResult mockQueryResult;

    private NeptuneSparqlConnection neptuneSparqlConnection;

    @Before
    public void setUp()
    {
        MockitoAnnotations.openMocks(this);
        neptuneSparqlConnection = new NeptuneSparqlConnection(LOCALHOST, PORT, false, REGION);
        neptuneSparqlConnection.connection = mockConnection;
        when(mockConnection.prepareTupleQuery(any(QueryLanguage.class), anyString())).thenReturn(mockTupleQuery);
        when(mockTupleQuery.evaluate()).thenReturn(mockQueryResult);
    }

    @Test
    public void runQuery_WithMultipleConditions_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(MULTIPLE_CONDITIONS_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, MULTIPLE_CONDITIONS_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithNestedConditions_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(NESTED_CONDITIONS_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, NESTED_CONDITIONS_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithOrderBy_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(ORDER_BY_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, ORDER_BY_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithLimit_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(LIMIT_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, LIMIT_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithDefaultLimit_ExecutesQuery()
    {
        String expectedQueryWithLimit = DEFAULT_QUERY + "\n" + Constants.SPARQL_QUERY_LIMIT;
        neptuneSparqlConnection.runQuery(expectedQueryWithLimit);
        verify(mockConnection).prepareTupleQuery(eq(QueryLanguage.SPARQL),
                argThat(q -> q.contains(DEFAULT_QUERY) && q.contains(Constants.SPARQL_QUERY_LIMIT)));
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithTopN_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(TOP_N_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, TOP_N_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithMixedDataTypes_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(MIXED_DATA_TYPES_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, MIXED_DATA_TYPES_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithNotEqualConditions_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(NOT_EQUAL_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, NOT_EQUAL_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithLongValues_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(LONG_VALUES_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, LONG_VALUES_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithBooleanLogic_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(BOOLEAN_LOGIC_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, BOOLEAN_LOGIC_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithFloatComparisons_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(FLOAT_COMPARISONS_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, FLOAT_COMPARISONS_QUERY);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void runQuery_WithMultipleOrderBy_ExecutesQuery()
    {
        neptuneSparqlConnection.runQuery(MULTIPLE_ORDER_BY_QUERY);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, MULTIPLE_ORDER_BY_QUERY);
        verify(mockTupleQuery).evaluate();
    }
}
