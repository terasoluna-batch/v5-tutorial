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
package org.terasoluna.batch.tutorial.exceptionhandlingwithtrycatch.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Component;
import org.terasoluna.batch.tutorial.common.dto.MemberInfoDto;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Component
public class PointAddTasklet implements Tasklet {

    private static final String TARGET_STATUS = "1";

    private static final String INITIAL_STATUS = "0";

    private static final String GOLD_MEMBER = "G";

    private static final String NORMAL_MEMBER = "N";

    private static final int MAX_POINT = 1000000;

    private static final int CHUNK_SIZE = 10;

    private static final Logger logger = LoggerFactory.getLogger(PointAddTasklet.class);

    @Inject
    MessageSource messageSource;

    @Inject
    ItemStreamReader<MemberInfoDto> reader;

    @Inject
    ItemWriter<MemberInfoDto> writer;

    @Inject
    Validator<MemberInfoDto> validator;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        MemberInfoDto item = null;

        List<MemberInfoDto> items = new ArrayList<>(CHUNK_SIZE);
        int errorCount = 0;

        try {
            reader.open(chunkContext.getStepContext().getStepExecution().getExecutionContext());
            while ((item = reader.read()) != null) {
                try {
                    validator.validate(item);
                } catch (ValidationException e) {
                    logger.warn(messageSource.getMessage("errors.maxInteger", new String[] { "point", "1000000" },
                            Locale.getDefault()));
                    errorCount++;
                    continue;
                }

                if (TARGET_STATUS.equals(item.getStatus())) {
                    if (GOLD_MEMBER.equals(item.getType())) {
                        item.setPoint(item.getPoint() + 100);
                    } else if (NORMAL_MEMBER.equals(item.getType())) {
                        item.setPoint(item.getPoint() + 10);
                    }

                    if (item.getPoint() > MAX_POINT) {
                        item.setPoint(MAX_POINT);
                    }

                    item.setStatus(INITIAL_STATUS);
                }

                items.add(item);

                if (items.size() == CHUNK_SIZE) {
                    writer.write(items);
                    items.clear();
                }
            }

            writer.write(items);
        } finally {
            reader.close();
        }
        if (errorCount > 0) {
            contribution.setExitStatus(new ExitStatus("SKIPPED"));
        }
        return RepeatStatus.FINISHED;
    }
}
