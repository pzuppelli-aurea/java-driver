/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

@RunWith(Enclosed.class)
public class ProtocolBetaVersionTest {

    public static class TestAutoUpgrade extends BetaVersionTest {
        @Override
        public Cluster.Builder createClusterBuilder() {
            return super.createClusterBuilder().setAllowBetaProtocolVersions(false);
        }
    }

    public static class TestConnectToBeta extends BetaVersionTest {
        @Override
        public Cluster.Builder createClusterBuilder() {
            return super.createClusterBuilder().setAllowBetaProtocolVersions(false);
        }
    }

    @CCMConfig(options = {"--install-dir=/Users/oleksandrpetrov/foss/java/cassandra/"})
    public static abstract class BetaVersionTest extends CCMTestsSupport {

        @Test(groups = "short")
        public void protocolVersionTest() throws Exception {
            Session session = session();

            session.execute("CREATE TABLE IF NOT EXISTS simpletable (\n" +
                    "        a int PRIMARY KEY,\n" +
                    "        b int,\n" +
                    "        c int," +
                    "        d int)");

            for (int i = 0; i < 10; i++) {
                session.execute("INSERT INTO simpletable (a,b,c,d) VALUES (?,?,?,?)", i, i, i, i);
            }
            ResultSet resultSet = session.execute("SELECT * FROM simpletable");

            assertEquals(10, resultSet.all().size());
        }
    }
}
