'use strict';

const Should = require('should');

const MysqlServiceFactory = require('../../lib/mysqlServiceFactory');
const MysqlDatabaseSetupFactory = require('../../lib/testing-helper/mysqlDatabaseSetupFactory');


const sourceDbConnection = {
    host: 'localhost',
    database: 'hapiestmysql',
    user: 'hapiestmysql',
    password: 'hapiestmysql',
    connectionLimit: 1
};
const mysqlServiceForSourceDb = MysqlServiceFactory.createFromObjWithOnePool(sourceDbConnection);

const testDbConnection = {
    host: 'localhost',
    database: 'hapiestmysql_test',
    user: 'hapiestmysql',
    password: 'hapiestmysql',
    connectionLimit: 1,
    multipleStatements: true
};

const mysqlServiceForTestDb = MysqlServiceFactory.createFromObjWithOnePool(testDbConnection);
const mysqlDatabaseSetup = MysqlDatabaseSetupFactory.create(mysqlServiceForTestDb, 'hapiestmysql');

describe('MysqlDatabaseSetup', function() {

    describe('replicateAllTables', function() {

        before(setupSourceDatabase);
        after(cleanupDatabases);

        it('should work', function(done) {
            
            mysqlDatabaseSetup.replicateAllTables((err) => {
                mysqlServiceForTestDb.selectAll(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${testDbConnection.database}'`)
                    .then(tableNameRows => tableNameRows.map(tableNameRow => tableNameRow.TABLE_NAME))
                    .then(tables => {
                        Should.exist(tables);
                        tables.should.be.an.Array();
                        tables.length.should.eql(3);

                        done(err);
                    })
                ;
            });

            /*
            return mysqlDatabaseSetup.replicateAllTables()
                .then(replicatedTables => {
                    console.log('*** Getting table list ***');
                    return mysqlServiceForTestDb.selectAll(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${testDbConnection.database}'`)
                        .then(tableNameRows => tableNameRows.map(tableNameRow => tableNameRow.TABLE_NAME));
                })
                .then(confirmedTables => {
                    console.log('*** Checking table list ***');
                    Should.exist(confirmedTables);
                    confirmedTables.should.be.an.Array();
                    confirmedTables.length.should.eql(3);
                });
                */

        });

    });

});

function setupSourceDatabase(done) {
    const queriesContext = _dropTables(mysqlServiceForSourceDb)
        .then(() => {}, (err) => done(err))
        .then(() => _createTables(mysqlServiceForSourceDb))
        .then(() => done(), (err) => done(err))
    ;
}

function cleanupDatabases(done) {
    const queriesContext = _dropTables(mysqlServiceForSourceDb)
        .then(() => {}, (err) => done(err))
        .then(() => _dropTables(mysqlServiceForTestDb))
        .then(() => done(), (err) => done(err))
}

/**
 * @param {MysqlService} mysqlService
 * @private
 */
function _dropTables(mysqlService) {
    const dropTableQueries = [
        `SET foreign_key_checks = 0;`,
        'DROP TABLE IF EXISTS users;',
        `DROP TABLE IF EXISTS videos;`,
        `DROP TABLE IF EXISTS user_downloads;`,
        `SET foreign_key_checks = 1;`
    ];

    return mysqlService.executeQueries(dropTableQueries);
}

/**
 * @param {MysqlService} mysqlService
 * @private
 */
function _createTables(mysqlService) {
    const createQueries = [
`CREATE TABLE IF NOT EXISTS users (
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(255) NOT NULL,
            date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE INDEX (email)
);`,
`CREATE TABLE IF NOT EXISTS videos (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(50) NOT NULL,
    url VARCHAR(255) NOT NULL
);`,
`CREATE TABLE IF NOT EXISTS user_downloads (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    video_id INT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (video_id) REFERENCES videos (id)
);`
    ];

    return mysqlService.executeQueries(createQueries);
}