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

import groovy.sql.Sql
import org.apache.commons.dbcp2.BasicDataSource
import org.dbunit.Assertion
import org.dbunit.database.DatabaseConfig
import org.dbunit.database.DatabaseDataSourceConnection
import org.dbunit.database.IDatabaseConnection
import org.dbunit.dataset.Column
import org.dbunit.dataset.DefaultTable
import org.dbunit.dataset.IDataSet
import org.dbunit.dataset.ITable
import org.dbunit.dataset.ReplacementDataSet
import org.dbunit.dataset.filter.DefaultColumnFilter
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder
import org.dbunit.ext.h2.H2DataTypeFactory
import org.dbunit.operation.DatabaseOperation
import org.dbunit.util.fileloader.CsvDataFileLoader
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.datasource.init.ScriptUtils

import java.sql.Timestamp
import java.time.Clock

/**
 * Utility for DBUnit.
 */
class DBUnitUtil {

    final Sql sql
    final BasicDataSource ds
    final IDatabaseConnection conn
    final ConfigObject conf
    final String[] tables
    final String[] dropSqlFilePaths
    final String[] createSqlFilePaths
    static final clock = Clock.systemDefaultZone()

    DBUnitUtil(String database = 'job') {
        conf = new ConfigSlurper().parse(
                new ClassPathResource('db-config.groovy').URL).dbunit."${database}"
        ds = new BasicDataSource(
                driverClassName: conf.driver,
                url: conf.url,
                username: conf.user,
                password: conf.pass)
        sql = new Sql(ds)
        conn = new DatabaseDataSourceConnection(ds, conf.schema as String)
        if (conf.driver.contains('h2')) {
            conn.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new H2DataTypeFactory())
        }
        tables = conf.tables
        dropSqlFilePaths = conf.dropSqlFilePaths
        createSqlFilePaths = conf.createSqlFilePaths

    }

    def close(){

        try {
            conn.close()
        } catch (Exception e) {
            // do nothing
        }
        try {
            ds.close()
        } catch (Exception e) {
            // do nothing
        }
        try {
            sql.close()
        } finally {
            // do nothing
        }
    }

    def cleanInsert(IDataSet dataSet) {
        DatabaseOperation.CLEAN_INSERT.execute(conn, dataSet)
    }

    def insert(IDataSet dataSet) {
        DatabaseOperation.INSERT.execute(conn, dataSet)
    }

    def update(IDataSet dataSet) {
        DatabaseOperation.UPDATE.execute(conn, dataSet)
    }

    def delete(IDataSet dataSet) {
        DatabaseOperation.DELETE.execute(conn, dataSet)
    }

    def deleteAll(IDataSet dataSet) {
        DatabaseOperation.DELETE_ALL.execute(conn, dataSet)
    }

    def truncate(IDataSet dataSet) {
        DatabaseOperation.TRUNCATE_TABLE.execute(conn, dataSet)
    }

    def deleteAll(String[] tableNames) {
        DatabaseOperation.DELETE_ALL.execute(conn, createDataSet(tableNames))
    }

    def truncate(String[] tableNames) {
        DatabaseOperation.TRUNCATE_TABLE.execute(conn, createDataSet(tableNames))
    }

    def refresh(IDataSet dataSet) {
        DatabaseOperation.REFRESH.execute(conn, dataSet)
    }

    def cleanAllTables() {
        deleteAll(tables)
    }

    def dropAndCreateTable() {
        executeSqlScript(dropSqlFilePaths)
        executeSqlScript(createSqlFilePaths)
    }

    def executeSqlScript(String[] scriptFiles) {
        scriptFiles.each {
            ScriptUtils.executeSqlScript(conn.connection, new ClassPathResource(it))
        }
    }

    static def assertEquals(ITable expectedTable, ITable actualTable) {
        Assertion.assertEquals(expectedTable, actualTable)
        true
    }

    static def assertEquals(IDataSet expectedDataSet, IDataSet actualDataSet) {
        Assertion.assertEquals(expectedDataSet, actualDataSet)
        true
    }

    static IDataSet createDataSetFormXML(String fileName) {
        new FlatXmlDataSetBuilder().build(new FileInputStream(fileName))
    }

    static IDataSet createDataSetFormCSV(String fileName) {
        new CsvDataFileLoader().loadDataSet(new File(fileName).toURI().toURL())
    }

    static ITable createDynamicTable(ITable src, int startIndex, int rowCount) {

        def tableName = src.tableMetaData.tableName
        def columns = src.tableMetaData.columns

        DefaultTable genTable = new DefaultTable(tableName, columns)

        def index = startIndex;
        rowCount.times {
            def columnValues = []
            columns.each {
                columnValues.add(src.getValue(index, it.columnName))
            }
            genTable.addRow(columnValues.toArray())
            index++;
        }

        genTable
    }

    IDataSet createDataSet(String[] tableNames) {
        return conn.createDataSet(tableNames)
    }

    ITable getTable(String tableName) {
        return createDataSet(tableName).getTable(tableName)
    }

    ITable getIncludedColumnsTable(String tableName, String[] columns) {
        return DefaultColumnFilter.includedColumnsTable(getTable(tableName), columns)
    }

    ITable getIncludedColumnsTable(String tableName, Column[] columns) {
        return DefaultColumnFilter.includedColumnsTable(getTable(tableName), columns)
    }

    ITable getIncludedColumnsTable(String targetTable, String includeTable) {
        return DefaultColumnFilter.includedColumnsTable(getTable(targetTable),
                getTable(includeTable).getTableMetaData().getColumns())
    }

    ITable getExcludedColumnsTable(String targetTable, String[] excludeColumns) {
        return DefaultColumnFilter.excludedColumnsTable(getTable(targetTable), excludeColumns)
    }

    ITable getExcludedColumnsTable(String targetTable, Column[] excludeColumns) {
        return DefaultColumnFilter.excludedColumnsTable(getTable(targetTable), excludeColumns)
    }

    ITable getExcludedColumnsTable(String targetTable, ITable excludeTable) {
        return DefaultColumnFilter.excludedColumnsTable(getTable(targetTable),
                excludeTable.getTableMetaData().getColumns())
    }

    ITable createQueryTable(String resultName, String sql) {
        return conn.createQueryTable(resultName, sql)
    }

    static final Map<String, Closure> defaultReplaceRule = [
            '[null]': { null },
            '[now]' : { new Timestamp(clock.millis()) }
    ].asImmutable()

    static ReplacementDataSet createDataSet(Closure c, Map<String, Closure> replaceRule = defaultReplaceRule) {
        def replacementDataSet = new ReplacementDataSet(DataTableLoader.loadDataSet(c))
        replaceRule.each { key, value -> replacementDataSet.addReplacementObject(key, value.call()) }
        replacementDataSet
    }

}
