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
import org.terasoluna.batch.async.db.AsyncBatchDaemon
import org.terasoluna.batch.tutorial.util.DBUnitUtil
import org.terasoluna.batch.tutorial.util.JobLauncher
import org.terasoluna.batch.tutorial.util.JobRequest
import org.terasoluna.batch.tutorial.util.LogCondition
import org.terasoluna.batch.tutorial.util.MongoUtil
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files

@Slf4j
class AsyncExecutionJobSpec extends Specification {
    @Rule
    TestName testName = new TestName()

    @Shared
            launcher = new JobLauncher()
    @Shared
            mongoUtil = new MongoUtil()
    @Shared
            adminDB = new DBUnitUtil('admin')

    def setupSpec() {
        adminDB.dropAndCreateTable()
    }

    def setup() {
        log.debug("### Spec case of [{}]", testName.methodName)
        adminDB.dropAndCreateTable()
        mongoUtil.deleteAll()
        def stopFile = launcher.stopFile()
        Files.deleteIfExists(stopFile.toPath())
    }

    def cleanupSpec() {
        mongoUtil.close()
        adminDB.close()
    }

    def "execute async batch dbaccess Tasklet"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddTasklet")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-dbaccess-tasklet.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddTasklet.step01]'))
        then:
        executeLog.size() == 1

    }

    def "execute async batch dbaccess Chunk"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddChunk")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-dbaccess-chunk.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddChunk.step01]'))
        then:
        executeLog.size() == 1

    }

    def "execute async batch fileaccess Tasklet"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddTasklet",
                jobParameter: "inputFile=files/input/input-member-info-data.csv,outputFile=files/output/output-member-info-data_tasklet.csv")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-fileaccess-tasklet.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddTasklet.step01]'))
        then:
        executeLog.size() == 1

    }

    def "execute async batch fileaccess Chunk"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddChunk",
                jobParameter: "inputFile=files/input/input-member-info-data.csv,outputFile=files/output/output-member-info-data_chunk.csv")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-fileaccess-chunk.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddChunk.step01]'))
        then:
        executeLog.size() == 1

    }

    def "execute async batch validation Tasklet"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddTasklet")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* -Dtutorial.insert-data.script=file:sqls/insert-member-info-error-data.sql ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-validation-tasklet.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddTasklet.step01]'))
        then:
        executeLog.size() == 1

    }

    def "execute async batch validation Chunk"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddChunk")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* -Dtutorial.insert-data.script=file:sqls/insert-member-info-error-data.sql ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-validation-chunk.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddChunk.step01]'))
        then:
        executeLog.size() == 1

    }

    def "execute async batch exceptionhandlingwithtrycatch Tasklet"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddTasklet")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* -Dtutorial.insert-data.script=file:sqls/insert-member-info-error-data.sql ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-exceptionhandlingwithtrycatch-tasklet.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddTasklet.step01]'))
        then:
        executeLog.size() == 1

    }

    def "execute async batch exceptionhandlingwithtrycatch Chunk"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddChunk")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* -Dtutorial.insert-data.script=file:sqls/insert-member-info-error-data.sql ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-exceptionhandlingwithtrycatch-chunk.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddChunk.step01]'))
        then:
        executeLog.size() == 1

    }

    def "execute async batch exceptionhandlingwithlistener Tasklet"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddTasklet")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* -Dtutorial.insert-data.script=file:sqls/insert-member-info-error-data.sql ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-exceptionhandlingwithlistener-tasklet.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddTasklet.step01]'))
        then:
        executeLog.size() == 1

    }

    def "execute async batch exceptionhandlingwithlistener Chunk"() {
        setup:
        def jobRequest = new JobRequest(jobName: "jobPointAddChunk")
        def jobSeqId = launcher.registerAsyncJob(adminDB, jobRequest)
        def command = "java -cp target/*${File.pathSeparator}target/dependency/* -Dtutorial.insert-data.script=file:sqls/insert-member-info-error-data.sql ${AsyncBatchDaemon.class.name} META-INF/asyncJobExecution/async-exceptionhandlingwithlistener-chunk.xml"
        when:
        def p = JobLauncher.executeProcess(command)
        launcher.waitAsyncJob(adminDB, jobSeqId)
        launcher.stopAsyncBatchDaemon(p)
        mongoUtil.waitForOutputLog(new LogCondition(message: 'Async Batch Daemon stopped after all jobs completed.'))
        def executeLog = mongoUtil.find(new LogCondition(message: 'Executing step: [jobPointAddChunk.step01]'))
        then:
        executeLog.size() == 1

    }
}
