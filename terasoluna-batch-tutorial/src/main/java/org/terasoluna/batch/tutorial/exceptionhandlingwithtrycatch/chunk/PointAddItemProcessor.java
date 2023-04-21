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
package org.terasoluna.batch.tutorial.exceptionhandlingwithtrycatch.chunk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Component;
import org.terasoluna.batch.tutorial.common.dto.MemberInfoDto;

import jakarta.inject.Inject;
import java.util.Locale;

@Component
public class PointAddItemProcessor implements ItemProcessor<MemberInfoDto, MemberInfoDto> {
    private static final String TARGET_STATUS = "1";

    private static final String INITIAL_STATUS = "0";

    private static final String GOLD_MEMBER = "G";

    private static final String NORMAL_MEMBER = "N";

    private static final int MAX_POINT = 1000000;

    private static final Logger logger = LoggerFactory.getLogger(PointAddItemProcessor.class);

    @Inject
    MessageSource messageSource;

    @Inject
    Validator<MemberInfoDto> validator;

    @Override
    public MemberInfoDto process(MemberInfoDto item) throws Exception {
        try {
            validator.validate(item);
        } catch (ValidationException e) {
            logger.warn(messageSource.getMessage("errors.maxInteger", new String[] { "point", "1000000" }, Locale
                    .getDefault()));
            return null;
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

        return item;
    }
}
