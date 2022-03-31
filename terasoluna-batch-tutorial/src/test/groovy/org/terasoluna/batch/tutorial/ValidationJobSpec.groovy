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
import org.springframework.batch.core.step.AbstractStep
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.terasoluna.batch.tutorial.util.DBUnitUtil
import org.terasoluna.batch.tutorial.util.JobLauncher
import org.terasoluna.batch.tutorial.util.JobRequest
import org.terasoluna.batch.tutorial.util.LogCondition
import org.terasoluna.batch.tutorial.util.MongoUtil
import spock.lang.Shared
import spock.lang.Specification

@Slf4j
class ValidationJobSpec extends Specification {

    @Shared
            mongoUtil = new MongoUtil()
    @Shared
            launcher = new JobLauncher()
    @Shared
            jobDB = new DBUnitUtil('job')

    def expectInitDataset = DBUnitUtil.createDataSet {
        member_info {
            id | type | status | point
            '00000001' | 'G' | '0' | 0
            '00000002' | 'N' | '0' | 0
            '00000003' | 'G' | '1' | 10
            '00000004' | 'N' | '1' | 10
            '00000005' | 'G' | '0' | 100
            '00000006' | 'N' | '0' | 100
            '00000007' | 'G' | '1' | 1000
            '00000008' | 'N' | '1' | 1000
            '00000009' | 'G' | '0' | 10000
            '00000010' | 'N' | '0' | 10000
            '00000011' | 'G' | '1' | 100000
            '00000012' | 'N' | '1' | 100000
            '00000013' | 'G' | '1' | 1000001
            '00000014' | 'N' | '1' | 999991
            '00000015' | 'G' | '1' | 999901
        }
    }

    def expectDatasetChunk = DBUnitUtil.createDataSet {
        member_info {
            id | type | status | point
            '00000001' | 'G' | '0' | 0
            '00000002' | 'N' | '0' | 0
            '00000003' | 'G' | '0' | 110
            '00000004' | 'N' | '0' | 20
            '00000005' | 'G' | '0' | 100
            '00000006' | 'N' | '0' | 100
            '00000007' | 'G' | '0' | 1100
            '00000008' | 'N' | '0' | 1010
            '00000009' | 'G' | '0' | 10000
            '00000010' | 'N' | '0' | 10000
            '00000011' | 'G' | '1' | 100000
            '00000012' | 'N' | '1' | 100000
            '00000013' | 'G' | '1' | 1000001
            '00000014' | 'N' | '1' | 999991
            '00000015' | 'G' | '1' | 999901
        }
    }

    def expectDatasetTasklet = expectInitDataset

    def setup() {
        log.debug("### Spec case of [{}]", this.specificationContext.currentIteration.displayName)
        mongoUtil.deleteAll()
    }

    def cleanupSpec() {
        mongoUtil.close()
        jobDB.close()
    }

    def "Initialize the database with SQL including error record"() {
        when:
        System.setProperty("tutorial.insert-data.script", "file:sqls/insert-member-info-error-data.sql")
        ApplicationContext context = new ClassPathXmlApplicationContext('META-INF/spring/job-base-context.xml')

        then:
        def actualTable = jobDB.getTable("member_info")
        def expectTable = expectInitDataset.getTable("member_info")
        DBUnitUtil.assertEquals(expectTable, actualTable)

        cleanup:
        context.close()
        System.setProperty("tutorial.insert-data.script", "file:sqls/insert-member-info-data.sql")
    }

    def "Confirm that Validate ChunkModel Job has encountered ValidationException."() {
        when:
        def exitCode = launcher.syncJob {
            JobLauncher.SyncJobArg arg ->
                arg.jobRequest = new JobRequest(
                        jobName: 'jobPointAddChunk',
                        jobFilePath: 'META-INF/jobs/validation/jobPointAddChunk.xml')
                arg.sysprop = ['tutorial.insert-data.script=file:sqls/insert-member-info-error-data.sql']
        }
        then:
        exitCode == 255

        def log = mongoUtil.find(new LogCondition(
                level: 'ERROR',
                logger: AbstractStep.class.name,
                message: ~/Encountered an error executing step/))
        log.size() == 1
        log.get(0).throwable._class.name == 'org.springframework.batch.item.validator.ValidationException'

        def actualTable = jobDB.getTable('member_info')
        def expectTable = expectDatasetChunk.getTable('member_info')
        DBUnitUtil.assertEquals(expectTable, actualTable)
    }

    def "Confirm that Validate TaskletModel Job has encountered ValidationException."() {
        when:
        def exitCode = launcher.syncJob {
            JobLauncher.SyncJobArg arg ->
                arg.jobRequest = new JobRequest(
                        jobName: 'jobPointAddTasklet',
                        jobFilePath: 'META-INF/jobs/validation/jobPointAddTasklet.xml')
                arg.sysprop = ['tutorial.insert-data.script=file:sqls/insert-member-info-error-data.sql']
        }
        then:
        exitCode == 255

        def log = mongoUtil.find(new LogCondition(
                level: 'ERROR',
                logger: AbstractStep.class.name,
                message: ~/Encountered an error executing step/))
        log.size() == 1
        log.get(0).throwable._class.name == 'org.springframework.batch.item.validator.ValidationException'

        def actualTable = jobDB.getTable('member_info')
        def expectTable = expectDatasetTasklet.getTable('member_info')
        DBUnitUtil.assertEquals(expectTable, actualTable)
    }
}
