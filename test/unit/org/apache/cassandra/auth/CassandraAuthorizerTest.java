/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.auth;

import java.util.Collections;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.ListPermissionsStatement;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.auth.AuthenticatedUser.SYSTEM_USER;

public class CassandraAuthorizerTest extends CQLTester
{
    private static final RoleResource PARENT_ROLE = RoleResource.role("parent");
    private static final RoleResource CHILD_ROLE = RoleResource.role("child");
    private static final RoleResource OTHER_ROLE = RoleResource.role("other");

    @BeforeClass
    public static void setupClass()
    {
        CQLTester.setUpClass();
        SchemaLoader.setupAuth(new RoleTestUtils.LocalCassandraRoleManager(),
                               new PasswordAuthenticator(),
                               new RoleTestUtils.LocalCassandraAuthorizer(),
                               new RoleTestUtils.LocalCassandraNetworkAuthorizer());
    }

    @Before
    public void beforeEachTest()
    {
        useSuperUser();

        executeNet(format("DROP ROLE IF EXISTS %s", PARENT));
        executeNet(format("DROP ROLE IF EXISTS %s", CHILD));
        executeNet(format("DROP ROLE IF EXISTS %s", OTHER));
    }

    @Test
    public void testListPermissionsOfChildByParent()
    {
        // create parent role by system user
        DatabaseDescriptor.getRoleManager()
                          .createRole(SYSTEM_USER, PARENT_ROLE, RoleTestUtils.getLoginRoleOptions());

        // create child role by parent
        String createRoleQuery = String.format("CREATE ROLE %s", CHILD_ROLE.getRoleName());
        CreateRoleStatement createRoleStatement = (CreateRoleStatement) QueryProcessor.parseStatement(createRoleQuery)
                                                                                      .prepare(ClientState.forInternalCalls());
        createRoleStatement.execute(getClientState(PARENT_ROLE.getRoleName()));

        // grant SELECT permission on ALL KEYSPACES to child
        DatabaseDescriptor.getAuthorizer()
                          .grant(SYSTEM_USER,
                                 Collections.singleton(Permission.SELECT),
                                 DataResource.root(),
                                 CHILD_ROLE);

        // list child permissions by parent
        String listPermissionsQuery = String.format("LIST ALL PERMISSIONS OF %s", CHILD_ROLE.getRoleName());
        ListPermissionsStatement listPermissionsStatement = (ListPermissionsStatement) QueryProcessor.parseStatement(listPermissionsQuery)
                                                                                                     .prepare(ClientState.forInternalCalls());
        ResultMessage message = listPermissionsStatement.execute(getClientState(PARENT_ROLE.getRoleName()));
        assertRows(UntypedResultSet.create(((ResultMessage.Rows) message).result),
                   row("child", "child", "<all keyspaces>", "SELECT"));

        // list child permissions by other user that is not their parent
        DatabaseDescriptor.getRoleManager().createRole(SYSTEM_USER, OTHER_ROLE, RoleTestUtils.getLoginRoleOptions());
        Assertions.assertThatThrownBy(() -> listPermissionsStatement.execute(getClientState(OTHER_ROLE.getRoleName())))
                  .isInstanceOf(UnauthorizedException.class)
                  .hasMessage("You are not authorized to view child's permissions");
    }

    private static ClientState getClientState(String username)
    {
        ClientState state = ClientState.forInternalCalls();
        state.login(new AuthenticatedUser(username));
        return state;
    }

    @Test
    public void testListDoesNotLeakRoleExistenceToUnauthorizedUsers()
    {
        useSuperUser();

        // A low-privilege login role with no DESCRIBE on the root roles resource, plus an unrelated
        // role it is not authorized to view.
        executeNet(format("CREATE ROLE %s WITH login=true AND password='%s'", CHILD, PASSWORD));
        executeNet(format("CREATE ROLE %s WITH login=true AND password='%s'", OTHER, PASSWORD));

        // An authorized caller (superuser) still gets the friendly existence error for a missing role.
        assertInvalidMessageNet("doesn't exist", "LIST ROLES OF nonexistent_role");
        assertInvalidMessageNet("doesn't exist", "LIST ALL PERMISSIONS OF nonexistent_role");

        useUser(CHILD, PASSWORD);

        // Unauthorized caller: a non-existent role and an existing-but-unviewable role must be
        // indistinguishable - both "not authorized", never "doesn't exist".
        assertInvalidMessageNet("You are not authorized to view roles granted to nonexistent_role",
                                "LIST ROLES OF nonexistent_role");
        assertInvalidMessageNet(format("You are not authorized to view roles granted to %s", OTHER),
                                format("LIST ROLES OF %s", OTHER));

        assertInvalidMessageNet("You are not authorized to view nonexistent_role's permissions",
                                "LIST ALL PERMISSIONS OF nonexistent_role");
        assertInvalidMessageNet(format("You are not authorized to view %s's permissions", OTHER),
                                format("LIST ALL PERMISSIONS OF %s", OTHER));
    }
}
