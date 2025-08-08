/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.sqlserver;

public final class SqlServerConstants
{
    public static final String NAME = "sqlserver";
    public static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final int DEFAULT_PORT = 1433;
    public static final String SQLSERVER_QUOTE_CHARACTER = "\"";
    static final String PARTITION_NUMBER = "partition_number";
    
    // OAuth related constants
    public static final String CLIENT_ID = "client_id";
    public static final String CLIENT_SECRET = "client_secret";
    public static final String TENANT_ID = "tenant_id";
    public static final String ACCESS_TOKEN = "access_token";
    public static final String FETCHED_AT = "fetched_at";
    public static final String EXPIRES_IN = "expires_in";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String USER = "user";
    
    private SqlServerConstants() {}
}
