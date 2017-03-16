'use strict';

// @TODO port this to a generic repository with it's own database

const Should = require('should');
const Promise = require('bluebird');
const Moment = require('moment');
const Sinon = require('sinon');
const interceptStdout = require('intercept-stdout');

const MysqlServiceFactory = require('../../lib/mysqlServiceFactory');
const MysqlService = require('../../lib/mysqlService');

const MysqlModificationResult = require('../../lib/mysqlModificationResult');
const LoggerFactory = require('hapiest-logger/lib/loggerFactory');
const LoggerConfigFactory = require('hapiest-logger/lib/loggerConfigFactory');
const loggerConfig = LoggerConfigFactory.createFromJsObj({enabled: true, consoleTransport: {enabled:true, level: 'info'}});
const logger = LoggerFactory.createLogger(loggerConfig);

/**********************************************************
 * COMMON SETUP
 **********************************************************/

// @TODO: moves this elsewhere perhaps
const writeConnectionConfig = {
    host: 'localhost',
    database: 'hapiestmysql',
    user: 'hapiestmysql',
    password: 'hapiestmysql',
    connectionLimit: 1,
    timezone: 'utc'
};
const readConnectionConfig = {
    host: ['localhost','localhost'],
    database: 'hapiestmysql',
    user: 'hapiestmysql',
    password: 'hapiestmysql',
    connectionLimit: 1,
    timezone: 'utc'
};
let mysqlService;

function databaseSetup() {

    const queries = [
        'DROP TABLE IF EXISTS __testing',
        `
                CREATE TABLE IF NOT EXISTS __testing (
                    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    colInt INT NOT NULL,
                    colVarchar VARCHAR(20) NOT NULL,
                    date_created_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    date_created_datetime DATETIME NULL,
                    date_created_date DATE NULL,
                    UNIQUE INDEX (colVarchar)
                );
            `,
        `
                INSERT INTO __testing (colInt, colVarchar, date_created_datetime, date_created_date)
                VALUES 
                    (1,'one', NOW(), NOW()),
                    (2, 'two', NOW(), NOW()),
                    (3, 'three', NOW(), NOW())
            `,
        `INSERT INTO __testing (colInt, colVarchar, date_created_datetime, date_created_timestamp) VALUES (4, 'four', '2016-10-10 00:00:00', '2016-10-10 12:00:00')`
    ];

    mysqlService = MysqlServiceFactory.createFromObj(writeConnectionConfig, readConnectionConfig, logger);
    return mysqlService.executeQueries(queries);
}

function databaseTeardown() {
    const queries = ['DROP TABLE IF EXISTS __testing'];
    return mysqlService.executeQueries(queries);
}

/**********************************************************
 * TESTS
 **********************************************************/

describe('MysqlService', function() {

    describe('select functions', function(){
        before(databaseSetup);
        after(databaseTeardown);

        describe('selectOne', function() {

            it('It should select a single row with an object result', function() {
                return mysqlService.selectOne('SELECT id, colInt, colVarchar, date_created_timestamp, date_created_datetime, date_created_date FROM __testing ORDER BY id ASC')
                    .then((result) => {
                        Should.exist(result);
                        result.should.be.an.object;
                        result.should.have.property('id');
                        result.id.should.be.a.number;
                        result.should.have.property('colInt');
                        result.colInt.should.eql(1);
                        result.should.have.property('colVarchar');
                        result.colVarchar.should.eql('one');

                        // Confirm that we convert dates back to dates (and don't leave them hanging as strings);
                        result.date_created_timestamp.should.be.an.instanceOf(Date);
                        result.date_created_datetime.should.be.an.instanceOf(Date);
                        result.date_created_date.should.be.an.instanceOf(Date);
                    });

            });

            it('It should return null when there are no results', function() {
                return mysqlService.selectOne('SELECT id, colInt, colVarchar FROM __testing WHERE colInt = \'twenty\' ORDER BY id ASC')
                    .then((result) => {
                        Should.not.exist(result);
                    });

            });

            it('Should use all available read pools randomly', function() {
                // Creating this locally so stubbing functions don't leak to other tests
                const localMysqlService = MysqlServiceFactory.createFromObj(writeConnectionConfig, readConnectionConfig, logger);
                const querySpy0 = Sinon.spy(localMysqlService._readPools[0], 'query');
                const querySpy1 = Sinon.spy(localMysqlService._readPools[1], 'query');

                const queriesToRun = [];
                for (let i=0; i<10; i++) {
                    queriesToRun.push('SELECT id, colInt, colVarchar, date_created_timestamp, date_created_datetime, date_created_date FROM __testing ORDER BY id ASC');
                }

                return Promise.map(queriesToRun, query => localMysqlService.selectOne(query))
                .then(results => {
                    Should.exist(results);
                    results.should.be.an.Array();
                    results.length.should.eql(10);

                    querySpy0.callCount.should.be.greaterThan(0);
                    querySpy1.callCount.should.be.greaterThan(0);
                });
            });

            describe('Date values with timezone config', function() {

                it('Should properly pull dates using UTC timezone provided in DB config', function() {
                    return mysqlService.selectOne("SELECT date_created_datetime, date_created_timestamp FROM __testing WHERE colInt = 4")
                        .then(row => {
                            row.date_created_datetime.should.be.an.instanceOf(Date);
                            row.date_created_timestamp.should.be.an.instanceOf(Date);

                            const expectedDateCreatedDatetime = Moment.utc('2016-10-10 00:00:00');
                            Moment(row.date_created_datetime).isSame(expectedDateCreatedDatetime).should.be.True();

                            const expectedDateCreatedTimestamp = Moment.utc('2016-10-10 12:00:00');
                            Moment(row.date_created_timestamp).isSame(expectedDateCreatedTimestamp).should.be.True();
                        })
                });

                it('Should properly pull dates using local timezone when a timezone is not specified in config', function() {
                    const writeConfig = JSON.parse(JSON.stringify(writeConnectionConfig));
                    const readConfig = JSON.parse(JSON.stringify(readConnectionConfig));
                    delete writeConfig.timezone;
                    delete readConfig.timezone;

                    const mysqlServiceTz = MysqlServiceFactory.createFromObj(writeConfig, readConfig, logger);
                    return mysqlServiceTz.selectOne("SELECT date_created_datetime, date_created_timestamp FROM __testing WHERE colInt = 4")
                    .then(row => {
                        row.date_created_datetime.should.be.an.instanceOf(Date);
                        row.date_created_timestamp.should.be.an.instanceOf(Date);
                        
                        const expectedDateCreatedDatetime = Moment(new Date('2016-10-10 00:00:00'));
                        Moment(row.date_created_datetime).isSame(expectedDateCreatedDatetime).should.be.True();

                        const expectedDateCreatedTimestamp = Moment(new Date('2016-10-10 12:00:00'));
                        Moment(row.date_created_timestamp).isSame(expectedDateCreatedTimestamp).should.be.True();
                    })
                });

                it('Should properly pull dates using America/Los_Angeles timezone when a timezone is not specified in config', function() {
                    this.skip(); // Right now node-mysql does not support full IANA timezones

                    const writeConfig = JSON.parse(JSON.stringify(writeConnectionConfig));
                    const readConfig = JSON.parse(JSON.stringify(readConnectionConfig));
                    writeConfig.timezone = 'America/Los_Angeles';
                    readConfig.timezone = 'America/Los_Angeles';

                    const mysqlServiceTz = MysqlServiceFactory.createFromObj(writeConfig, readConfig, logger);
                    return mysqlServiceTz.selectOne("SELECT date_created_datetime, date_created_timestamp FROM __testing WHERE colInt = 4")
                    .then(row => {
                        row.date_created_datetime.should.be.an.instanceOf(Date);
                        row.date_created_timestamp.should.be.an.instanceOf(Date);

                        const expectedDateCreatedDatetime = Moment(new Date('2016-10-10 03:00:00'));
                        Moment(row.date_created_datetime).isSame(expectedDateCreatedDatetime).should.be.True();

                        const expectedDateCreatedTimestamp = Moment(new Date('2016-10-10 15:00:00'));
                        Moment(row.date_created_timestamp).isSame(expectedDateCreatedTimestamp).should.be.True();
                    })
                });

            });

        });

        // @TODO: figure out how to test read from master...need to set up separate databases perhaps?
        describe('selectOneFromMaster', function() {
            it('It should select a single row with an object result', function() {
                return mysqlService.selectOneFromMaster('SELECT id, colInt, colVarchar FROM __testing ORDER BY id ASC')
                    .then((result) => {
                        Should.exist(result);
                        result.should.be.an.object;
                        result.should.have.property('id');
                        result.id.should.be.a.number;
                        result.should.have.property('colInt');
                        result.colInt.should.eql(1);
                        result.should.have.property('colVarchar');
                        result.colVarchar.should.eql('one');
                    });

            });

            it('It should return null when there are no results', function() {
                return mysqlService.selectOneFromMaster('SELECT id, colInt, colVarchar FROM __testing WHERE colInt = \'twenty\' ORDER BY id ASC')
                    .then((result) => {
                        Should.not.exist(result);
                    });

            });
        });

        describe('selectAll', function() {
            it('It should return an array of object results', function() {
                return mysqlService.selectAll('SELECT colInt, colVarchar FROM __testing WHERE colInt IN (2,3) ORDER BY id ASC')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.an.array;
                        results.length.should.eql(2);

                        results[0].should.not.have.property('id');
                        results[0].should.have.property('colInt');
                        results[0].colInt.should.eql(2);
                        results[0].should.have.property('colVarchar');
                        results[0].colVarchar.should.eql('two');

                        results[1].should.not.have.property('id');
                        results[1].should.have.property('colInt');
                        results[1].colInt.should.eql(3);
                        results[1].should.have.property('colVarchar');
                        results[1].colVarchar.should.eql('three');
                    });

            });

            it('It should return an empty array when there are no results', function() {
                return mysqlService.selectAll('SELECT id, colInt, colVarchar FROM __testing WHERE colInt = \'twenty\' ORDER BY id ASC')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.an.array;
                        results.length.should.eql(0);
                    });

            });
        });

        // @TODO: figure out how to test read from master...need to set up separate databases perhaps?
        describe('selectAllFromMaster', function() {
            it('It should return an array of object results', function() {
                return mysqlService.selectAllFromMaster('SELECT colInt, colVarchar FROM __testing WHERE colInt IN (2,3) ORDER BY id ASC')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.an.array;
                        results.length.should.eql(2);

                        results[0].should.not.have.property('id');
                        results[0].should.have.property('colInt');
                        results[0].colInt.should.eql(2);
                        results[0].should.have.property('colVarchar');
                        results[0].colVarchar.should.eql('two');

                        results[1].should.not.have.property('id');
                        results[1].should.have.property('colInt');
                        results[1].colInt.should.eql(3);
                        results[1].should.have.property('colVarchar');
                        results[1].colVarchar.should.eql('three');
                    });

            });

            it('It should return an empty array when there are no results', function() {
                return mysqlService.selectAllFromMaster('SELECT id, colInt, colVarchar FROM __testing WHERE colInt = \'twenty\' ORDER BY id ASC')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.an.array;
                        results.length.should.eql(0);
                    });

            });
        });

        describe('stream results', function() {
          const results = [];
          it('It should be able to stream the results of a query', function(done) {
            mysqlService.streamQuery('SELECT colInt, colVarchar FROM __testing WHERE colInt IN (2,3) ORDER BY id ASC')
              .on('data', (result) => {
                results.push(result);
              }).on('end', () => {
                results.should.be.an.array;
                results.length.should.eql(2);

                results[0].should.not.have.property('id');
                results[0].should.have.property('colInt');
                results[0].colInt.should.eql(2);
                results[0].should.have.property('colVarchar');
                results[0].colVarchar.should.eql('two');

                results[1].should.not.have.property('id');
                results[1].should.have.property('colInt');
                results[1].colInt.should.eql(3);
                results[1].should.have.property('colVarchar');
                results[1].colVarchar.should.eql('three');
                done();
              });
          })
        })
          it('It should pass errors through the stream', function(done) {
            let errorEmitted = false;
            let dataEmitted = false;
            mysqlService.streamQuery('SELECT ERROR SYNTAX...')
              .on('data', () => dataEmitted = true)
              .on('error', (error) => {
                  errorEmitted = true;
                  error.should.have.property('code');
                  error.code.should.eql('ER_PARSE_ERROR');
              })
              .on('end', () => {
                  errorEmitted.should.be.true;
                  dataEmitted.should.be.false;
                  done();
              });
          });
    });

    describe('data modification functions', function() {
        describe('insert', function() {
            beforeEach(databaseSetup);
            after(databaseTeardown);

            it('Should insert a value into the database and return data about the insertion', function() {
                return mysqlService.insert('INSERT INTO __testing(colInt, colVarchar) VALUES(10, \'ten\')')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.instanceof(MysqlModificationResult);
                        results.affectedRows.should.eql(1);
                        results.insertId.should.eql(5);
                    })
                    .then(() => mysqlService.selectOne('SELECT colInt, colVarchar FROM __testing WHERE colInt = 10'))
                    .then((results) => { // Confirm the insert actually happened
                        Should.exist(results);
                        results.should.deepEqual({colInt: 10, colVarchar: 'ten'});
                    });
            });

            it('Should insert multiple values into the database and return data about the insertion', function() {
                return mysqlService.insert('INSERT INTO __testing(colInt, colVarchar) VALUES (10, \'ten\'), (20, \'twenty\')')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.instanceof(MysqlModificationResult);
                        results.affectedRows.should.eql(2);
                        results.insertId.should.eql(5);
                    })
                    .then(() => mysqlService.selectAll('SELECT id, colInt, colVarchar FROM __testing WHERE colInt IN (10,20)'))
                    .then((results) => {
                        Should.exist(results);

                        results.should.deepEqual([
                            {id: 5, colInt: 10, colVarchar: 'ten'},
                            {id: 6, colInt: 20, colVarchar: 'twenty'}
                        ]);
                    });
            });

            it('Should throw an error on duplicate key', function() {
                return mysqlService.insert('INSERT INTO __testing(colInt, colVarchar) VALUES(10, \'two\')')
                    .should.be.rejectedWith(Error);
            });
        });

        describe('update', function() {
            beforeEach(databaseSetup);
            after(databaseTeardown);

            it('Should update a value in the database and return data about the update', function() {
                return mysqlService.update('UPDATE __testing SET colVarchar = \'twenty\' WHERE colInt = 2')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.instanceof(MysqlModificationResult);
                        results.affectedRows.should.eql(1);
                        results.changedRows.should.eql(1);
                    });
            });

            it('Should update three values in the database and return data about the update', function() {
                return mysqlService.update('UPDATE __testing SET colInt = 3')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.instanceof(MysqlModificationResult);
                        results.affectedRows.should.eql(4);
                        results.changedRows.should.eql(3);
                    });
            });

            it('Should throw an error on duplicate key', function() {
                return mysqlService.update('UPDATE __testing SET colVarchar = \'three\' WHERE colInt = 2')
                    .should.be.rejectedWith(Error);
            });
        });

        describe('upsert', function() {
            beforeEach(databaseSetup);
            after(databaseTeardown);

            it('Should insert a new value in the database', function() {
                return mysqlService.upsert('INSERT INTO __testing(colInt, colVarchar) VALUES(9, \'nine\') ON DUPLICATE KEY UPDATE colInt = 9, colVarchar = \'nine\'')
                    .then(results => {
                        Should.exist(results);
                        results.should.be.instanceof(MysqlModificationResult);
                        results.affectedRows.should.eql(1);
                    });
            });

            it('Should update an existing value in the database', function() {
                return mysqlService.upsert('INSERT INTO __testing(colInt, colVarchar) VALUES(9, \'nine\') ON DUPLICATE KEY UPDATE colInt = 9, colVarchar = \'nine\'')
                    .then(results => {
                        Should.exist(results);
                        results.should.be.instanceof(MysqlModificationResult);
                        results.affectedRows.should.eql(1);
                        results.changedRows.should.eql(0);
                    })
                    .then(() => mysqlService.upsert('INSERT INTO __testing(colInt, colVarchar) VALUES(10, \'nine\') ON DUPLICATE KEY UPDATE colInt = 99, colVarchar = \'nine\''))
                    .then(results => {
                        Should.exist(results);
                        results.should.be.instanceof(MysqlModificationResult);
                        results.affectedRows.should.eql(2);  // MySQL returns 1 for an insert, 2 if the update happens instead
                    });
            });
        });

        describe('delete', function() {
            beforeEach(databaseSetup);
            after(databaseTeardown);

            it('Should delete a row in the database and return data about the delete', function() {
                return mysqlService.delete('DELETE FROM __testing WHERE colInt = 2')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.instanceof(MysqlModificationResult);
                        results.affectedRows.should.eql(1);
                    });
            });

            it('Should delete three rows in the database and return data about the delete', function() {
                return mysqlService.delete('DELETE FROM __testing WHERE id > 1')
                    .then((results) => {
                        Should.exist(results);
                        results.should.be.instanceof(MysqlModificationResult);
                        results.affectedRows.should.eql(3);
                    });
            });

            it('Should throw an error on safe update failure', function() {
                return mysqlService.delete('DELETE FROM __testing WHERE x = y')
                    .should.be.rejectedWith(Error);
            });
        });
    });

    // @TODO: add tests for the generic query functions

    describe('clean', function() {
        it('Should escape little Bobby DROP TABLEs', function() {
            const input = "little Bobby '; DROP TABLES;";
            const cleanedInput = mysqlService.clean(input);

            cleanedInput.should.eql("'little Bobby \\'; DROP TABLES;'");
        });
        
        it('Should convert an array of strings to comma-delimited list of escaped strings', function() {
            const input = ["string 1", "string 2", "string 3 'and then some' "];
            const cleanedInput = mysqlService.clean(input);
            cleanedInput.should.eql("'string 1', 'string 2', 'string 3 \\'and then some\\' '");
        })
    });

    describe('_logError', function() {
        it('Should call .error function on the logger', function() {
            let loggedTxt = '';
            let unhookIntercept = null;
            let error = null;
            return Promise.resolve()
                .then(() => {
                    unhookIntercept = interceptStdout((txt) => {loggedTxt += txt;}); // Need this to intercept the logger's output to stdout
                })
                .then(() => mysqlService.selectOne('SELECT ERROR SYNTAX...'))
                .catch((err) => {
                    error = err;
                })
                .then(() => {
                    Should.exist(error);
                    const loggedObj = JSON.parse(loggedTxt);
                    
                    loggedObj.should.have.property('message');
                    loggedObj.should.have.property('level');
                    loggedObj.should.have.property('data');
                    
                    loggedObj.message.should.eql('Error executing SQL');
                    loggedObj.level.should.eql('error');
                    
                    loggedObj.data.should.have.property('err');
                    loggedObj.data.should.have.property('errorStack');
                    loggedObj.data.should.have.property('errorMessage');
                    loggedObj.data.should.have.property('sql');

                    loggedObj.data.errorStack.should.be.a.String();
                    loggedObj.data.errorMessage.should.be.a.String();
                    loggedObj.data.sql.should.eql('SELECT ERROR SYNTAX...');
                })
        });
    });

    describe('ping', function() {

        it('Should ping a valid connection successfully', function() {
            return mysqlService.ping();
        });

        it('Should error on invalid write connection', function() {
            const invalidConfig = {
                host: 'localhost',
                database: 'invalid',
                user: 'hapiestmysql',
                password: 'hapiestmysql',
                connectionLimit: 1
            };
            const invalidMysqlService = MysqlServiceFactory.createFromObjWithOnePool(invalidConfig, logger);
            return invalidMysqlService.ping()
                .catch(err => {
                    Should.exist(err);
                    (err.code).should.equal('ER_DBACCESS_DENIED_ERROR')
                })
        });

        it('Should error on invalid read connection', function() {
            const invalidReadConnectionConfig = {
                host: ['localhost','invalid'],
                database: 'hapiestmysql',
                user: 'hapiestmysql',
                password: 'hapiestmysql',
                connectionLimit: 1
            };
            const invalidMysqlService = MysqlServiceFactory.createFromObj(writeConnectionConfig, invalidReadConnectionConfig, logger);
            return invalidMysqlService.ping()
                .catch(err => {
                    Should.exist(err);
                    (err.code).should.equal('ENOTFOUND')
                });
        });
    });

    describe('endPools', function() {
        it('Should end all connection pools', function() {
            return mysqlService.endPools()
                .then(() => {
                    mysqlService._writePool._closed.should.be.true;
                    mysqlService._readPools.forEach(pool => pool._closed.should.be.true);
                });
        })
    });

    describe('resetPools', function() {
        it('Should reset the available pools based on write and read configurations', function() {
            return mysqlService.resetPools()
                .then(() => {
                    mysqlService._writePool._closed.should.be.true;
                    mysqlService._readPools.forEach(pool => pool._closed.should.be.true);
                });
        });
    });

})
