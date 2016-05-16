'use strict';

// @TODO port this to a generic repository with it's own database

const Should = require('should');
const Promise = require('bluebird');
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
    connectionLimit: 1
};
const mysqlService = MysqlServiceFactory.createFromObjWithOnePool(writeConnectionConfig, logger);

function databaseSetup(done) {

    const queries = [
        'DROP TABLE IF EXISTS __testing',
        `
                CREATE TABLE IF NOT EXISTS __testing (
                    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    colInt INT NOT NULL,
                    colVarchar VARCHAR(20) NOT NULL,
                    UNIQUE CONSTRAIN (colVarchar)
                );
            `,
        `
                INSERT INTO __testing (colInt, colVarchar)
                VALUES 
                    (1,'one'),
                    (2, 'two'),
                    (3, 'three')
            `
    ];

    mysqlService.executeQueries(queries)
        .then(() => done(), (err) => done(err));
}

function databaseTeardown(done) {
    const queries = ['DROP TABLE IF EXISTS __testing'];
    mysqlService.executeQueries(queries)
        .then(() => done(), (err) => done(err));
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
                return mysqlService.selectOne('SELECT id, colInt, colVarchar FROM __testing ORDER BY id ASC')
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
                return mysqlService.selectOne('SELECT id, colInt, colVarchar FROM __testing WHERE colInt = \'twenty\' ORDER BY id ASC')
                    .then((result) => {
                        Should.not.exist(result);
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
                        results.insertId.should.eql(4);
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
                        results.insertId.should.eql(4);
                    })
                    .then(() => mysqlService.selectAll('SELECT id, colInt, colVarchar FROM __testing WHERE colInt IN (10,20)'))
                    .then((results) => {
                        Should.exist(results);

                        results.should.deepEqual([
                            {id: 4, colInt: 10, colVarchar: 'ten'},
                            {id: 5, colInt: 20, colVarchar: 'twenty'}
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
                        results.affectedRows.should.eql(3);
                        results.changedRows.should.eql(2);
                    });
            });

            it('Should throw an error on duplicate key', function() {
                return mysqlService.update('UPDATE __testing SET colVarchar = \'three\' WHERE colInt = 2')
                    .should.be.rejectedWith(Error);
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
                        results.affectedRows.should.eql(2);
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


});