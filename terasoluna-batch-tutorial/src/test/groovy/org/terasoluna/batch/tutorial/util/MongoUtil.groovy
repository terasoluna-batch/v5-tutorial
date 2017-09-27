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
package org.terasoluna.batch.tutorial.util

import com.gmongo.GMongo
import com.mongodb.*
import org.springframework.core.io.ClassPathResource

import java.util.concurrent.TimeoutException

/**
 * Utility for MongoDB. support function for functionaltest-case only.
 */
class MongoUtil {

    final GMongo mongo
    final DB db
    final DBCollection col

    MongoUtil() {
        def conf = new ConfigSlurper().parse(
                new ClassPathResource('mongo-config.groovy').URL).mongoutil
        mongo = new GMongo(conf.host as String, conf.port as int)
        db = mongo.getDB(conf.database as String)
        col = this.db.getCollection(conf.collection as String)
    }

    def find(Map condition=[:]){
        col.find(BasicDBObjectBuilder.start(condition).get())
    }

    static Map convertToMap(LogCondition c) {
        def map = [:]
        if (c == null) {
            return map
        }
        c.properties.findAll {
            it.value != null && it.key != 'class'
        }
    }

    static LogCursor convertToLogCursor(DBObject dbObject) {
        if (dbObject == null) {
            return null
        }
        def map = dbObject as Map
        def throwable = map.remove('throwable') as Map
        if (throwable != null) {
            def throwableLog = convertThrowable(throwable)
            map['throwable'] = throwableLog
        }
        new LogCursor(map)
    }

    static LogCursor.ThrowableLog convertThrowable(Map map) {
        if (map == null) {
            return null
        }
        def _class = map.remove('class')
        map['_class'] = _class
        def cause = map.remove('cause') as Map
        def throwableLog = new LogCursor.ThrowableLog(map)
        if (cause != null) {
            throwableLog.cause = convertThrowable(cause)
        }
        throwableLog
    }

    List<LogCursor> find(LogCondition c) {
        def list = []
        def cursor = find(convertToMap(c))
        cursor.forEach { list.add(convertToLogCursor(it)) }
        list
    }

    def findOne(Map condition=[:]){
        col.findOne(condition)
    }

    LogCursor findOne(LogCondition c) {
        convertToLogCursor(findOne(convertToMap(c)))
    }

    def count(Map condition=[:]){
        col.count(BasicDBObjectBuilder.start(condition).get())
    }

    def count(LogCondition c) {
        count(convertToMap(c))
    }

    def deleteAll() {
        col.remove(new BasicDBObject())
    }

    def close() {
        mongo.close()
    }

    LogCursor waitForOutputLog(long timeout = 60*1000L, LogCondition cond) {
        long startTime = System.currentTimeMillis()
        LogCursor cursor
        while (true) {
            cursor = findOne(cond)
            if (cursor != null) {
                break
            }
            long currentTime = System.currentTimeMillis()
            long nowTime = currentTime - startTime
            if (timeout > 0L && nowTime > timeout) {
                throw new TimeoutException(
                        "*** exceeded waiting time: ${timeout} starttime: ${startTime}")
            }
            sleep(500L)
        }
        cursor
    }
}
