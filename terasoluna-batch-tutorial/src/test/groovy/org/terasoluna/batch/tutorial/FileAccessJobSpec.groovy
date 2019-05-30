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
import org.slf4j.LoggerFactory
import org.terasoluna.batch.tutorial.util.JobLauncher
import org.terasoluna.batch.tutorial.util.JobRequest
import spock.lang.Shared
import spock.lang.Specification

@Slf4j
class FileAccessJobSpec extends Specification {
    @Rule
    TestName testName = new TestName()

    @Shared
            log = LoggerFactory.getLogger(FileAccessJobSpec)

    @Shared
            jobLauncher = new JobLauncher()

    def setup() {
        log.debug("### Spec case of [{}]", testName.methodName)
        if (!new File(outputDir).deleteDir()) {
            throw new IllegalStateException("#deleteAll at setup-method failed.")
        }
        if (!new File(outputDir).mkdirs()) {
            throw new IllegalStateException("#mkdirs at setup-method failed.")
        }
    }

    def outputDir = "files/output"

    def inputData = "files/input/input-member-info-data.csv"
    def expectData = "files/expect/expect-member-info-data.csv"

    def "Confilm that The ChunkModel Job has completed successfully."() {
        setup:
        def outputData = outputDir + "/output-member-info-data_chunk.csv"

        when:
        int exitCode = jobLauncher.syncJob(new JobRequest(
                jobFilePath: 'META-INF/jobs/fileaccess/jobPointAddChunk.xml',
                jobName: 'jobPointAddChunk',
                jobParameter: "inputFile=${inputData} outputFile=${outputData}"))

        then:
        exitCode == 0

        new File(outputData).exists() == true

        def actual_data = new File(outputData).readLines()
        def input_data = new File(inputData).readLines()

        actual_data.size() == input_data.size()

        def expect_data = new File(expectData).readLines()

        actual_data == expect_data
    }

    def "Confilm that The TaskletModel Job has completed successfully."() {
        setup:
        def outputData = outputDir + "/output-member-info-data_tasklet.csv"

        when:
        int exitCode = jobLauncher.syncJob(new JobRequest(
                jobFilePath: 'META-INF/jobs/fileaccess/jobPointAddTasklet.xml',
                jobName: 'jobPointAddTasklet',
                jobParameter: "inputFile=${inputData} outputFile=${outputData}"))

        then:
        exitCode == 0

        new File(outputData).exists() == true

        def actual_data = new File(outputData).readLines()
        def input_data = new File(inputData).readLines()

        actual_data.size() == input_data.size()

        def expect_data = new File(expectData).readLines()

        actual_data == expect_data
    }
}
