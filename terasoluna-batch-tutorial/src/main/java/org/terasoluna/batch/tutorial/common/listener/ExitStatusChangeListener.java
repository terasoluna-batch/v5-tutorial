/*
 * Copyright (C) 2017 NTT DATA Corporation
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
package org.terasoluna.batch.tutorial.common.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class ExitStatusChangeListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        // do nothing.
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExitStatus exitStatus = stepExecution.getExitStatus();
        if (conditionalCheck(stepExecution)) {
            exitStatus = new ExitStatus("SKIPPED");
        }
        return exitStatus;
    }

    private boolean conditionalCheck(StepExecution stepExecution) {
        return (stepExecution.getWriteCount() != stepExecution.getReadCount());
    }
}
