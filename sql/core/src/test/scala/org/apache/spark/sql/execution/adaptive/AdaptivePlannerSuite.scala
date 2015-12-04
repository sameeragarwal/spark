/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.{Row, QueryTest}
import org.apache.spark.sql.catalyst.DefaultParserDialect
import org.apache.spark.sql.test.{SQLTestUtils, SharedSQLContext}


/** A SQL Dialect for testing purpose, and it can not be nested type */
class MyDialect extends DefaultParserDialect

class AdaptivePlannerSuite extends QueryTest with SQLTestUtils with SharedSQLContext {

  import testImplicits._

  setupTestData()

  // join does not work now
  ignore("adaptive query optimization") {
    withSQLConf("spark.sql.useAdaptivePlanner" -> "true") {
      Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str").registerTempTable("df")

      val query =
        """
          |SELECT x.str, COUNT(*)
          |FROM df x JOIN df y ON x.str = y.str
          |GROUP BY x.str
        """.stripMargin

      checkAnswer(sql(query), Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
    }
  }

  test("adaptive query optimization: join") {
    withSQLConf("spark.sql.useAdaptivePlanner" -> "true") {
      Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str").registerTempTable("df")

      val query =
        """
          |SELECT x.str, y.str
          |FROM df x JOIN df y ON x.str = y.str
        """.stripMargin

      checkAnswer(sql(query), Row("1", "1") :: Row("2", "2") :: Row("3", "3") :: Nil)
    }
  }
}