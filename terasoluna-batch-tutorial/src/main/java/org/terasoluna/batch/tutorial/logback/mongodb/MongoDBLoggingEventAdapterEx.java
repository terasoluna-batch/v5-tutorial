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
package org.terasoluna.batch.tutorial.logback.mongodb;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.contrib.mongodb.MongoDBLoggingEventAppender;
import com.mongodb.BasicDBObject;

/**
 * {@link MongoDBLoggingEventAppender} extends for functional functionaltest.
 *
 * @since 5.0.0
 */
public class MongoDBLoggingEventAdapterEx extends MongoDBLoggingEventAppender {

    /**
     * Remove filed of logging arguments.
     *
     * @param iLoggingEvent Log event.
     * @return Mongo db object.
     */
    @Override
    protected BasicDBObject toMongoDocument(ILoggingEvent iLoggingEvent) {
        BasicDBObject doc = super.toMongoDocument(iLoggingEvent);
        doc.removeField("arguments");
        return doc;
    }

}
