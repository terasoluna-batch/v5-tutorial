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
package org.terasoluna.batch.tutorial.fileaccess.tasklet;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.terasoluna.batch.tutorial.common.dto.MemberInfoDto;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@Component
@Scope("step")
public class PointAddTasklet implements Tasklet {

    private static final String TARGET_STATUS = "1";

    private static final String INITIAL_STATUS = "0";

    private static final String GOLD_MEMBER = "G";

    private static final String NORMAL_MEMBER = "N";

    private static final int MAX_POINT = 1000000;

    @Inject
    ItemStreamReader<MemberInfoDto> reader;

    @Inject
    ItemStreamWriter<MemberInfoDto> writer;

    private static final int CHUNK_SIZE = 10;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        MemberInfoDto item = null;

        List<MemberInfoDto> items = new ArrayList<>(CHUNK_SIZE);
        try {

            reader.open(chunkContext.getStepContext().getStepExecution().getExecutionContext());
            writer.open(chunkContext.getStepContext().getStepExecution().getExecutionContext());

            while ((item = reader.read()) != null) {

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
            try {
                reader.close();
            } catch (ItemStreamException e) {
                // ignore
            }
            try {
                writer.close();
            } catch (ItemStreamException e) {
                // ignore
            }
        }

        return RepeatStatus.FINISHED;
    }
}
