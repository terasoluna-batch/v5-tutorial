/*
 * Copyright (C) 2017 NTT DATA Corporation. Copyright (C) 2017 NTT Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.terasoluna.batch.tutorial

import groovy.util.logging.Slf4j
import org.junit.Rule
import org.junit.rules.TestName
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.terasoluna.batch.tutorial.util.DBUnitUtil
import org.terasoluna.batch.tutorial.util.JobLauncher
import org.terasoluna.batch.tutorial.util.JobRequest
import spock.lang.Shared
import spock.lang.Specification

@Slf4j
class DBAccessJobSpec extends Specification {
    @Rule
    TestName testName = new TestName()

    @Shared
            launcher = new JobLauncher()
    @Shared
            jobDB = new DBUnitUtil('job')

    def expectInitDataset = DBUnitUtil.createDataSet {
        member_info {
            id | type | status | point
            '00000001' | 'G' | '1' | 0
            '00000002' | 'N' | '1' | 0
            '00000003' | 'G' | '0' | 10
            '00000004' | 'N' | '0' | 10
            '00000005' | 'G' | '1' | 100
            '00000006' | 'N' | '1' | 100
            '00000007' | 'G' | '0' | 1000
            '00000008' | 'N' | '0' | 1000
            '00000009' | 'G' | '1' | 10000
            '00000010' | 'N' | '1' | 10000
            '00000011' | 'G' | '0' | 100000
            '00000012' | 'N' | '0' | 100000
            '00000013' | 'G' | '1' | 999901
            '00000014' | 'N' | '1' | 999991
            '00000015' | 'G' | '0' | 999900
            '00000016' | 'N' | '0' | 999990
            '00000017' | 'G' | '1' | 10
            '00000018' | 'N' | '1' | 10
            '00000019' | 'G' | '0' | 100
            '00000020' | 'N' | '0' | 100
            '00000021' | 'G' | '1' | 1000
            '00000022' | 'N' | '1' | 1000
            '00000023' | 'G' | '0' | 10000
            '00000024' | 'N' | '0' | 10000
            '00000025' | 'G' | '1' | 100000
            '00000026' | 'N' | '1' | 100000
            '00000027' | 'G' | '0' | 1000000
            '00000028' | 'N' | '0' | 1000000
            '00000029' | 'G' | '1' | 999899
            '00000030' | 'N' | '1' | 999989
        }
    }

    def expectDataset = DBUnitUtil.createDataSet {
        member_info {
            id | type | status | point
            '00000001' | 'G' | '0' | 100
            '00000002' | 'N' | '0' | 10
            '00000003' | 'G' | '0' | 10
            '00000004' | 'N' | '0' | 10
            '00000005' | 'G' | '0' | 200
            '00000006' | 'N' | '0' | 110
            '00000007' | 'G' | '0' | 1000
            '00000008' | 'N' | '0' | 1000
            '00000009' | 'G' | '0' | 10100
            '00000010' | 'N' | '0' | 10010
            '00000011' | 'G' | '0' | 100000
            '00000012' | 'N' | '0' | 100000
            '00000013' | 'G' | '0' | 1000000
            '00000014' | 'N' | '0' | 1000000
            '00000015' | 'G' | '0' | 999900
            '00000016' | 'N' | '0' | 999990
            '00000017' | 'G' | '0' | 110
            '00000018' | 'N' | '0' | 20
            '00000019' | 'G' | '0' | 100
            '00000020' | 'N' | '0' | 100
            '00000021' | 'G' | '0' | 1100
            '00000022' | 'N' | '0' | 1010
            '00000023' | 'G' | '0' | 10000
            '00000024' | 'N' | '0' | 10000
            '00000025' | 'G' | '0' | 100100
            '00000026' | 'N' | '0' | 100010
            '00000027' | 'G' | '0' | 1000000
            '00000028' | 'N' | '0' | 1000000
            '00000029' | 'G' | '0' | 999999
            '00000030' | 'N' | '0' | 999999
        }
    }

    def setup() {
        log.debug("### Spec case of [{}]", testName.methodName)
    }

    def cleanupSpec() {
        jobDB.close()
    }

    def "Initialize Database"() {
        when:
        ApplicationContext context = new ClassPathXmlApplicationContext('META-INF/spring/job-base-context.xml')

        then:
        def actualTable = jobDB.getTable("member_info")
        def expectTable = expectInitDataset.getTable("member_info")
        DBUnitUtil.assertEquals(expectTable, actualTable)

        cleanup:
        context.close()
    }

    def "Confirm that Dbaccess ChunkModel Job has completed successfully."() {
        when:
        int exitCode = launcher.syncJob(new JobRequest(
                jobFilePath: 'META-INF/jobs/dbaccess/jobPointAddChunk.xml',
                jobName: 'jobPointAddChunk'))
        then:
        exitCode == 0
        def actualTable = jobDB.getTable("member_info")
        def expectTable = expectDataset.getTable("member_info")
        DBUnitUtil.assertEquals(expectTable, actualTable)
    }

    def "Confirm that Dbaccess TaskletModel Job has completed successfully."() {
        when:
        int exitCode = launcher.syncJob(new JobRequest(
                jobFilePath: 'META-INF/jobs/dbaccess/jobPointAddTasklet.xml',
                jobName: 'jobPointAddTasklet'))
        then:
        exitCode == 0
        def actualTable = jobDB.getTable("member_info")
        def expectTable = expectDataset.getTable("member_info")
        DBUnitUtil.assertEquals(expectTable, actualTable)
    }
}

