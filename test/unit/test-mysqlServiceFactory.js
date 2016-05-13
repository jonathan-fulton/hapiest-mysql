'use strict';

const Should = require('should');
const Path = require('path');
const MysqlServiceFactory = require('../../lib/mysqlServiceFactory');
const MysqlService = require('../../lib/mysqlService');
const Async = require('async');

describe('MysqlServiceFactory', function() {

    describe('createFromNodeConfig', function() {

        it('Should load from config-1/test.json', function(done) {
            const nodeConfig = Internals.resetNodeConfig('config-1');
            const mysqlService = MysqlServiceFactory.createFromNodeConfig(nodeConfig);

            Should.exist(mysqlService);
            mysqlService.should.be.an.instanceOf(MysqlService);

            mysqlService.should.have.property('_writePool');
            mysqlService.should.have.property('_readPool');

            Async.auto({
                databaseSetup: Async.apply(Internals.databaseSetup, mysqlService),
                checkMysqlService: ['databaseSetup', function(results, next) {
                    mysqlService.selectAll('SELECT * FROM __testing')
                        .then(results => {
                            Should.exist(results);
                            results.should.be.an.Array();
                            results.length.should.eql(3);

                            results.should.deepEqual([
                                {id: 1, colInt: 1, colVarchar: 'one'},
                                {id: 2, colInt: 2, colVarchar: 'two'},
                                {id: 3, colInt: 3, colVarchar: 'three'}
                            ]);
                        })
                        .then(next)
                        .catch(err => next(err));
                }]
            }, (err, results) => {
                done(err);
            });
        });

    });

});

class Internals {

    /**
     * @param {string} configDirName
     */
    static resetNodeConfig(configDirName) {
        process.env.NODE_CONFIG_DIR = Path.resolve(__dirname, '../unit-helper/mysqlServiceFactory', configDirName);
        return require('config-uncached')(true);
    }

    static databaseSetup(mysqlService, done) {
        const queries = [
            'DROP TABLE IF EXISTS __testing',
            `
                CREATE TABLE __testing (
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

        let currentContext = Promise.resolve();
        queries.forEach(query => {
            currentContext = currentContext.then(() => mysqlService.executeGenericQuery(query));
        });
        currentContext.catch(err => done(err)).then(() => done());
    }

}