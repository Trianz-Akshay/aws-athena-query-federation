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
        neptuneSparqlConnection = new NeptuneSparqlConnection("localhost", "8182", false, "us-west-2");
        neptuneSparqlConnection.connection = mockConnection;
        when(mockConnection.prepareTupleQuery(any(QueryLanguage.class), anyString())).thenReturn(mockTupleQuery);
        when(mockTupleQuery.evaluate()).thenReturn(mockQueryResult);
    }

    @Test
    public void testComplexSparqlExpressionWithMultipleConditions()
    {
        String query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . FILTER (?age > 25 && ?salary < 100000) }";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithNestedConditions()
    {
        String query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . FILTER ((?age >= 18 && ?age <= 65) || ?status = 'admin') }";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithOrderBy()
    {
        String query = "SELECT ?s ?name ?age WHERE { ?s ?name ?age . FILTER(?age > 25) } ORDER BY ?name ASC, ?age DESC";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithLimit()
    {
        String query = "SELECT ?s ?name ?age WHERE { ?s ?name ?age . FILTER(?age > 25) } ORDER BY ?name ASC LIMIT 10";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithDefaultLimit()
    {
        String query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
        String expectedQueryWithLimit = query + "\n" + Constants.SPARQL_QUERY_LIMIT;
        neptuneSparqlConnection.runQuery(expectedQueryWithLimit);
        verify(mockConnection).prepareTupleQuery(eq(QueryLanguage.SPARQL),
                argThat(q -> q.contains(query) && q.contains(Constants.SPARQL_QUERY_LIMIT)));
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithTopN()
    {
        String query = "SELECT ?s ?name ?salary WHERE { ?s ?name ?salary . FILTER(?salary > 50000) } ORDER BY ?salary DESC LIMIT 5";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithMixedDataTypes()
    {
        String query = "SELECT ?s ?name ?age ?active WHERE { ?s ?name ?age ?active . FILTER(?name = 'John' && ?age > 25 && ?active = true) } ORDER BY ?name ASC, ?age DESC";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithNotEqualConditions()
    {
        String query = "SELECT ?s ?status ?role WHERE { ?s ?status ?role . FILTER(?status != 'inactive' && ?role != 'guest') } ORDER BY ?status ASC";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithLongValues()
    {
        String query = "SELECT ?s ?timestamp ?id WHERE { ?s ?timestamp ?id . FILTER(?timestamp > 1609459200000 && ?id != 0) } ORDER BY ?id ASC, ?timestamp DESC";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithBooleanLogic()
    {
        String query = "SELECT ?s ?isActive ?isVerified WHERE { ?s ?isActive ?isVerified . FILTER(?isActive = true && ?isVerified = false) } ORDER BY ?isActive ASC, ?isVerified DESC";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithFloatComparisons()
    {
        String query = "SELECT ?s ?price ?rating WHERE { ?s ?price ?rating . FILTER(?price >= 10.5 && ?rating <= 4.8) } ORDER BY ?price ASC, ?rating DESC";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
    
    @Test
    public void testComplexSparqlExpressionWithMultipleOrderBy()
    {
        String query = "SELECT ?s ?department ?salary ?name WHERE { ?s ?department ?salary ?name . FILTER(?salary > 50000) } ORDER BY ?department ASC, ?salary DESC, ?name ASC";
        neptuneSparqlConnection.runQuery(query);
        verify(mockConnection).prepareTupleQuery(QueryLanguage.SPARQL, query);
        verify(mockTupleQuery).evaluate();
    }
}
