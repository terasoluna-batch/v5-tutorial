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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Locale;

@Component
public class ChunkErrorLoggingListener implements ChunkListener {
    private static final Logger logger = LoggerFactory.getLogger(ChunkErrorLoggingListener.class);

    @Inject
    MessageSource messageSource;

    @Override
    public void beforeChunk(ChunkContext chunkContext) {
        // do nothing.
    }

    @Override
    public void afterChunk(ChunkContext chunkContext) {
        // do nothing.
    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {
        Exception e = (Exception) chunkContext.getAttribute(ChunkListener.ROLLBACK_EXCEPTION_KEY);
        if (e instanceof ValidationException) {
            logger.error(messageSource.getMessage("errors.maxInteger", new String[] { "Point", "1000000" }, Locale
                    .getDefault()));
        }
    }
}
