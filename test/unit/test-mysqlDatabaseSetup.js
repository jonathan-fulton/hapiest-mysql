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

        beforeEach(setupSourceDatabase);
        afterEach(cleanupDatabases);

        it('Should copy over three tables from the source database', function(done) {

            mysqlDatabaseSetup.replicateAllTables()
                .then(() => {
                    const sql = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${testDbConnection.database}'ORDER BY TABLE_NAME ASC`;
                    mysqlServiceForTestDb.selectAll(sql)
                        .then(tableNameRows => tableNameRows.map(tableNameRow => tableNameRow.TABLE_NAME))
                        .then(tables => {
                            Should.exist(tables);
                            tables.should.be.an.Array();
                            tables.length.should.eql(3);

                            tables[0].should.eql('users');
                            tables[1].should.eql('user_downloads');
                            tables[2].should.eql('videos');

                            done();
                        })
                        .catch(err => done(err));
                });
        });

    });

    describe('replicateTables', function() {

        beforeEach(setupSourceDatabase);
        afterEach(cleanupDatabases);

        it('Should copy over three tables from the source database', function(done) {

            mysqlDatabaseSetup.replicateTables(['users', 'videos'])
                .then(() => {
                    const sql = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${testDbConnection.database}'ORDER BY TABLE_NAME ASC`;
                    mysqlServiceForTestDb.selectAll(sql)
                        .then(tableNameRows => tableNameRows.map(tableNameRow => tableNameRow.TABLE_NAME))
                        .then(tables => {
                            Should.exist(tables);
                            tables.should.be.an.Array();
                            tables.length.should.eql(2);

                            tables[0].should.eql('users');
                            tables[1].should.eql('videos');

                            done();
                        })
                        .catch(err => done(err));
                });
        });

    });

    describe('copyDataForAllTables', function() {

        beforeEach(setupSourceDatabase);
        afterEach(cleanupDatabases);

        it('Should copy data for three tables from the source database', function(done) {

            mysqlDatabaseSetup.replicateAllTables()
                .then(() => {
                    return Promise.all([mysqlDatabaseSetup.copyDataForAllTables()])
                })
                .then(() => {
                    const sqlQueries = [`SELECT * FROM users`, `SELECT * FROM videos`, `SELECT * FROM user_downloads`];
                    mysqlServiceForTestDb.executeQueries(sqlQueries)
                        .then(results => {

                            Should.exist(results);
                            results.should.be.an.Array();
                            results.length.should.eql(3);

                            Should.exist(results[0][0]);
                            Should.not.exist(results[0][1]);
                            const userInfo = results[0][0];
                            userInfo.should.have.properties(['id', 'first_name', 'last_name', 'email', 'date_created']);
                            userInfo.id.should.eql(1);
                            userInfo.first_name.should.eql('John');
                            userInfo.last_name.should.eql('Doe');
                            userInfo.email.should.eql('john.doe@gmail.com');

                            Should.exist(results[1][0]);
                            Should.exist(results[1][1]);
                            Should.not.exist(results[1][2]);

                            const video1 = results[1][0];
                            video1.id.should.eql(1);
                            video1.title.should.eql('video 1');
                            video1.url.should.eql('http://www.youtube.com/video1');
                            const video2 = results[1][1];
                            video2.id.should.eql(2);
                            video2.title.should.eql('video 2');
                            video2.url.should.eql('http://www.youtube.com/video2');

                            Should.exist(results[2][0]);
                            Should.exist(results[2][1]);
                            Should.not.exist(results[2][2]);

                            const download1 = results[2][0];
                            download1.id.should.eql(1);
                            download1.user_id.should.eql(1);
                            download1.video_id.should.eql(1);
                            const download2 = results[2][1];
                            download2.id.should.eql(2);
                            download2.user_id.should.eql(1);
                            download2.video_id.should.eql(2);

                            done();
                        })
                        .catch(err => done(err));
                });
        });

    });

    describe('copyDataForTables', function() {

        beforeEach(setupSourceDatabase);
        afterEach(cleanupDatabases);

        it('Should copy data for only the user table', function(done) {

            mysqlDatabaseSetup.replicateAllTables()
                .then(() => {
                    return Promise.all([mysqlDatabaseSetup.copyDataForTables(['users'])])
                })
                .then(() => {
                    const sqlQueries = [`SELECT * FROM users`, `SELECT * FROM videos`, `SELECT * FROM user_downloads`];
                    mysqlServiceForTestDb.executeQueries(sqlQueries)
                        .then(results => {

                            Should.exist(results);
                            results.should.be.an.Array();
                            results.length.should.eql(3);

                            Should.exist(results[0][0]);
                            Should.not.exist(results[0][1]);
                            const userInfo = results[0][0];
                            userInfo.should.have.properties(['id', 'first_name', 'last_name', 'email', 'date_created']);
                            userInfo.id.should.eql(1);
                            userInfo.first_name.should.eql('John');
                            userInfo.last_name.should.eql('Doe');
                            userInfo.email.should.eql('john.doe@gmail.com');

                            Should.exist(results[1]); // No videos copied over
                            Should.not.exist(results[1][0]);

                            Should.exist(results[2]); // No downloads copied over
                            Should.not.exist(results[2][0]);

                            done();
                        })
                        .catch(err => done(err));
                });
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
        'DROP TABLE IF EXISTS __testing;', // Necessary due to table pollution from other tests...@TODO: fix this
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
`INSERT INTO users (first_name, last_name, email) VALUES ('John', 'Doe', 'john.doe@gmail.com');`,
`CREATE TABLE IF NOT EXISTS videos (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(50) NOT NULL,
    url VARCHAR(255) NOT NULL
);`,
`INSERT INTO videos (title, url) VALUES ('video 1', 'http://www.youtube.com/video1'), ('video 2', 'http://www.youtube.com/video2');`,
`CREATE TABLE IF NOT EXISTS user_downloads (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    video_id INT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (video_id) REFERENCES videos (id)
);`,
`INSERT INTO user_downloads (user_id, video_id) SELECT u.id as user_id, v.id as video_id FROM users u CROSS JOIN videos v;`
    ];

    return mysqlService.executeQueries(createQueries);
}