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
package org.terasoluna.batch.tutorial.util

import groovy.util.logging.Slf4j
import org.springframework.batch.core.launch.support.CommandLineJobRunner
import org.springframework.batch.core.launch.support.JvmSystemExiter
import org.springframework.batch.core.launch.support.SystemExiter
import org.springframework.core.io.ClassPathResource
import org.terasoluna.batch.async.db.AsyncBatchDaemon

import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * CAUTION: If you use APIs in IDE:
 *  <ol>
 *    <li> Please set to java working directory to "/v5-inner-tutorial/terasoluna-batch-tutorial"</li>
 *    <li> Change directory to /v5-inner-tutorial, Execute "mvn clean package" in advance.</li>
 *  </ol>
 * (If verify wholly by Maven, don't care above.)
 *
 * If you want to execute CommandLineJobRunner in no-fork, use system property: '-DJobLauncher.noFork' (available syncJob() only).
 * On the other hand, use '-DJobLauncher.remoteDebug' for activating remote debug (available at syncJob() and startAsyncBatchDaemon()).
 *
 * No-fork mode is restricted below:
 * <ol>
 *   <li>Environment values are ignored.</li>
 *   <li>CommandLineJobRunner works same process and thread with Specs.</li>
 * </ol>
 *
 * Remote debugging is restricted below:
 * <ol>
 *   <li>Using JDPA port:5005.</li>
 *   <li>Since jobLauncher process is executed different process from Specs, launching job process is executed concurrently and not joined in Spec.</li>
 * </ol>
 */
@Slf4j
class JobLauncher {

    File _stopFile

    static final REMOTE_DEBUG_OPTION = '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005'

    static final NO_FORK_PROPERTY = 'JobLauncher.noFork'

    static final REMOTE_DEBUG_PROPERTY = 'JobLauncher.remoteDebug'

    /**
     * Execute job synchronously.
     *
     * @param jobRequest target job information
     * @param timeout <ol><li>negative value: wait infinitely</li><li>0: return immediately</li><li>positive value: wait till timeout</li></ol>
     * @param timeUnit timeUnit(default milliseconds)
     * @param env Environment values (key=value format)
     * @param sysprop System properties (key=value format)
     * @return process exit code. (Integer.MAX_VALUE if return immediately)
     * @throws TimeoutException if exceeds timeout(only positive).
     */
    int syncJob(JobRequest jobRequest, long timeout = 10 * 1000L, TimeUnit timeUnit = TimeUnit.MILLISECONDS,
                String[] env = [], String[] sysprop = []) {
        assert jobRequest != null
        assert jobRequest.jobName != null

        if (System.getProperty(NO_FORK_PROPERTY) != null) {
            return executeNoFork(jobRequest, sysprop)
        }

        def remoteDebug = ''
        if (System.getProperty(REMOTE_DEBUG_PROPERTY) != null) {
            remoteDebug = REMOTE_DEBUG_OPTION
        }

        def systemProperties = ""
        for (String sp : sysprop) {
            systemProperties += " -D${sp}"
        }



        def command = "java -cp target/*${File.pathSeparator}target/dependency/* ${systemProperties} ${remoteDebug} ${CommandLineJobRunner.class.name} " +
                "${jobRequest.jobFilePath} ${jobRequest.jobName} ${jobRequest.jobParameter ?: ''}"

        def p = executeProcess(command, env, null)
        p.waitForOrKill(timeUnit.toMillis(timeout))
        p.exitValue()
    }

    int executeNoFork(JobRequest jobRequest, String[] sysprop) {
        sysprop?.each {
            def (key, value) = it?.split('=')
            if (key) {
                System.setProperty(key, value)
            }
        }
        def dummySystemExiter = new DummySystemExiter()
        CommandLineJobRunner.presetSystemExiter(dummySystemExiter)
        String[] args = [jobRequest.jobFilePath, jobRequest.jobName, jobRequest.jobParameter]
        try {
            CommandLineJobRunner.main(args)
        } finally {
            CommandLineJobRunner.presetSystemExiter(new JvmSystemExiter())
        }
        dummySystemExiter.status
    }

    /**
     * Execute job synchronously by overwriting specific arguments only.
     *
     * @param c arguments for synchronous job execution
     * @return process exit code. (Integer.MAX_VALUE if return immediately)
     */
    int syncJob(Closure c) {
        def syncJobArg = new SyncJobArg()
        c(syncJobArg)
        syncJob(syncJobArg.jobRequest, syncJobArg.timeout, syncJobArg.timeUnit, syncJobArg.env, syncJobArg.sysprop)
    }

    static class SyncJobArg {
        JobRequest jobRequest
        long timeout = 10 * 1000L
        TimeUnit timeUnit = TimeUnit.MILLISECONDS
        String[] env = []
        String[] sysprop = []
    }

    /**
     * Start up the AsyncBatchDaemon process.
     *
     * @param env Environment values. (key=value format)
     * @param sysprop System properties. (key=value format)
     * @return process
     */
    Process startAsyncBatchDaemon(String[] env = null, String[] sysprop = null) {
        if (stopFile().exists()) {
            Files.delete(stopFile().toPath())
        }

        def remoteDebug = ''
        if (System.getProperty(REMOTE_DEBUG_PROPERTY) != null) {
            remoteDebug = REMOTE_DEBUG_OPTION
        }

        def systemProperties = ""
        for (String sp : sysprop) {
            systemProperties += " -D${sp}"
        }

        def command = "java -cp target/*${File.pathSeparator}target/dependency/* ${systemProperties} ${remoteDebug} ${AsyncBatchDaemon.class.name}"
        executeProcess(command, env, null)
    }

    synchronized long registerAsyncJob(DBUnitUtil dbUnitUtil, JobRequest jobRequest) {
        def sql = dbUnitUtil.sql
        long jobSeqId
        def createDate = new Date().toTimestamp()
        sql.withTransaction {
            dbUnitUtil.insert(DataTableLoader.loadDataSet {
                batch_job_request {
                    job_name           | job_parameter           | polling_status | create_date
                    jobRequest.jobName | jobRequest.jobParameter | 'INIT'         | createDate
                }
            })
            def row = sql.firstRow("SELECT job_seq_id FROM batch_job_request WHERE job_name = ${jobRequest.jobName} " +
                    "AND job_parameter = ${jobRequest.jobParameter} AND create_date = ${createDate}")
            jobSeqId = row.get("job_seq_id")
        }
        jobSeqId
    }

    synchronized void waitAsyncJob(DBUnitUtil dbUnitUtil, long jobSeqId,
                                   long timeout = 30 * 1000L,
                                   TimeUnit timeUnit = TimeUnit.MILLISECONDS) {
        long startTime = System.currentTimeMillis()
        long limit = timeUnit.toMillis(timeout)
        while (true) {
            def row = dbUnitUtil.sql.firstRow("SELECT COUNT(job_seq_id) AS count FROM batch_job_request " +
                    "WHERE job_seq_id = ${jobSeqId} AND polling_status = 'EXECUTED'")
            def count = row.get('count') as Integer
            if (count > 0) {
                break
            }
            long currentTime = System.currentTimeMillis()
            if (limit > 0 && currentTime - startTime > limit) {
                throw new TimeoutException("exceeded time limit [timeout:${timeout}]")
            }
            sleep(500L)
        }
    }

    synchronized void stopAsyncBatchDaemon(Process p, long timeout = 30 * 1000L, TimeUnit timeUnit = TimeUnit.MILLISECONDS) {
        File f = stopFile()
        if (!f.exists() && !f.createNewFile()) {
            throw new IOException("fail to create stop file: ${f.absolutePath}")
        }
        if (p != null && p.alive) {
            p.waitForOrKill(timeUnit.toMillis(timeout))
        }
    }

    synchronized File stopFile() {
        if (_stopFile != null) {
            return _stopFile
        }
        // .properties file can not use ConfigSlurper because of https://issues.apache.org/jira/browse/GROOVY-4491
        def appProp = new Properties()
        new ClassPathResource('batch-application.properties').URL.withInputStream { inputStream ->
            appProp.load(inputStream)
        }
        _stopFile = new File(appProp['async-batch-daemon.polling-stop-file-path'] as String)
    }

    synchronized static Process executeProcess(String command, String[] env = null, File workingPath = null) {
        log.debug("*** start ext-process. [command:{}] [env:{}] [workingPath:{}]", command, env, workingPath)
        def p = command.execute(env, workingPath)
        p.consumeProcessOutput(System.out as OutputStream, System.err as OutputStream)
        p
    }

    static class DummySystemExiter implements SystemExiter {

        def status = 0

        @Override
        void exit(int status) {
            this.status = status
        }
    }
}
