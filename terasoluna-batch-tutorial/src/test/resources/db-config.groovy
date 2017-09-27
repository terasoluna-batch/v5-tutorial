dbunit {
    admin {
        driver = 'org.h2.Driver'
        url = 'jdbc:h2:~/batch-admin;AUTO_SERVER=TRUE'
        user = 'sa'
        pass = null
        schema = null
        tables = []
        dropSqlFilePaths = ['org/springframework/batch/core/schema-drop-h2.sql',
                            'org/terasoluna/batch/async/db/schema-drop-h2.sql']
        createSqlFilePaths = ['org/springframework/batch/core/schema-h2.sql',
                              'org/terasoluna/batch/async/db/schema-h2.sql']
    }
    job {
        driver = 'org.h2.Driver'
        url = 'jdbc:h2:~/batch-admin;AUTO_SERVER=TRUE'
        user = 'sa'
        pass = null
        schema = null
        tables = []
        dropSqlFilePaths = []
        createSqlFilePaths = []
    }
}
