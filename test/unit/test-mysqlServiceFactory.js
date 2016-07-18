'use strict';

const Should = require('should');
const Promise = require('bluebird');
const Path = require('path');
const MysqlServiceFactory = require('../../lib/mysqlServiceFactory');
const MysqlService = require('../../lib/mysqlService');
const NodeConfig = require('config-uncached');

describe('MysqlServiceFactory', function() {

    describe('createFromNodeConfig', function() {

        it('Should load from config-1/test.json', function() {
            const nodeConfig = Internals.resetNodeConfig('config-1');
            const mysqlService = MysqlServiceFactory.createFromNodeConfig(nodeConfig);

            Should.exist(mysqlService);
            mysqlService.should.be.an.instanceOf(MysqlService);

            mysqlService.should.have.property('_writePool');
            mysqlService.should.have.property('_readPools');

            return Promise.resolve()
            .then(() => Internals.databaseSetup(mysqlService))
            .then(() => mysqlService.selectAll('SELECT * FROM __testing'))
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
            .finally(() => Internals.databaseTeardown(mysqlService));
        });

        it('Should load from config-2/test.json', function() {
            const nodeConfig = Internals.resetNodeConfig('config-2');
            const mysqlService = MysqlServiceFactory.createFromNodeConfig(nodeConfig);

            Should.exist(mysqlService);
            mysqlService.should.be.an.instanceOf(MysqlService);

            mysqlService.should.have.property('_writePool');
            mysqlService.should.have.property('_readPools');

            mysqlService._writePool.should.eql(mysqlService._readPools[0]);

            return Promise.resolve()
            .then(() => Internals.databaseSetup(mysqlService))
            .then(() => mysqlService.selectAll('SELECT * FROM __testing'))
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
            .then(() => mysqlService.executeGenericQuery('SELECT 1 as answer; SELECT 1 as answer;'))
            .then(results => {
                Should.exist(results);
                results.should.be.an.Array();
                results.length.should.eql(2);



                results[0].should.be.an.Array();
                Should.exist(results[0][0]);
                Should.exist(results[0][0].answer);
                results[0][0].answer.should.eql(1);
                results[1].should.be.an.Array();
                Should.exist(results[1][0]);
                Should.exist(results[1][0].answer);
                results[0][0].answer.should.eql(1);
            })
            .finally(() => Internals.databaseTeardown(mysqlService));
        });

        it('Should load from config-3/test.json', function() {
            const nodeConfig = Internals.resetNodeConfig('config-3');
            const mysqlService = MysqlServiceFactory.createFromNodeConfig(nodeConfig);

            Should.exist(mysqlService);
            mysqlService.should.be.an.instanceOf(MysqlService);

            mysqlService.should.have.property('_writePool');
            mysqlService.should.have.property('_readPools');
            mysqlService._readPools.length.should.eql(2);

            return Promise.resolve()
            .then(() => Internals.databaseSetup(mysqlService))
            .then(() => mysqlService.selectAll('SELECT * FROM __testing'))
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
            .then(() => mysqlService.executeGenericQuery('SELECT 1 as answer; SELECT 1 as answer;'))
            .then(results => {
                Should.exist(results);
                results.should.be.an.Array();
                results.length.should.eql(2);



                results[0].should.be.an.Array();
                Should.exist(results[0][0]);
                Should.exist(results[0][0].answer);
                results[0][0].answer.should.eql(1);
                results[1].should.be.an.Array();
                Should.exist(results[1][0]);
                Should.exist(results[1][0].answer);
                results[0][0].answer.should.eql(1);
            })
            .finally(() => Internals.databaseTeardown(mysqlService));
        });

    });

});

class Internals {

    /**
     * @param {string} configDirName
     */
    static resetNodeConfig(configDirName) {
        process.env.NODE_CONFIG_DIR = Path.resolve(__dirname, '../unit-helper/mysqlServiceFactory', configDirName);
        const nodeConfig = NodeConfig(true);
        return nodeConfig;
    }

    /**
     * @param {MysqlService} mysqlService
     */
    static databaseSetup(mysqlService) {
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

        return mysqlService.executeQueries(queries)
    }

    /**
     * @param {MysqlService} mysqlService
     */
    static databaseTeardown(mysqlService) {
        const queries = ['DROP TABLE IF EXISTS __testing'];
        return mysqlService.executeQueries(queries);
    }



}