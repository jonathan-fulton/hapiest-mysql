'use strict';

const Should = require('should');
const Path = require('path');
const MysqlPoolConnectionsConfigFactory = require('../../lib/mysqlPoolConnectionConfigFactory');
const MysqlPoolConnectionConfig = require('../../lib/mysqlPoolConnectionConfig');

describe('MysqlPoolConnectionsConfigFactory', function() {

    describe('createWriteConnectionConfigFromNodeConfig', function() {

        it('Should load from config-1/test.json', function() {
            const nodeConfig = Internals.resetNodeConfig('config-1');
            const writeConfig = MysqlPoolConnectionsConfigFactory.createWriteConnectionConfigFromNodeConfig(nodeConfig);

            Should.exist(writeConfig);
            writeConfig.should.be.an.instanceOf(MysqlPoolConnectionConfig);

            writeConfig.host.should.eql('localhost');
            Should.not.exist(writeConfig.port);
            writeConfig.database.should.eql('optimage');
            writeConfig.user.should.eql('optimagedev');
            writeConfig.password.should.eql('=7QM^!4ynHebT7s7');
            writeConfig.connectionLimit.should.eql(10);
        });

        it('Should load from config-2/test.json', function() {
            const nodeConfig = Internals.resetNodeConfig('config-2');
            const writeConfig = MysqlPoolConnectionsConfigFactory.createWriteConnectionConfigFromNodeConfig(nodeConfig);

            Should.exist(writeConfig);
            writeConfig.should.be.an.instanceOf(MysqlPoolConnectionConfig);

            writeConfig.host.should.eql('somehost');
            writeConfig.port.should.eql(30000);
            writeConfig.database.should.eql('db');
            writeConfig.user.should.eql('user');
            writeConfig.password.should.eql('badpassword');
            writeConfig.connectionLimit.should.eql(100);
            Should.exist(writeConfig.multipleStatements);
            writeConfig.multipleStatements.should.be.true;
            writeConfig.timezone.should.eql('utc');
        });

    });

    describe('createReadConnectionConfigFromNodeConfig', function() {

        it('Should load from config-1/test.json', function() {
            const nodeConfig = Internals.resetNodeConfig('config-1');
            const readConfig = MysqlPoolConnectionsConfigFactory.createReadConnectionConfigFromNodeConfig(nodeConfig);

            Should.exist(readConfig);
            readConfig.should.be.an.instanceOf(MysqlPoolConnectionConfig);

            readConfig.host.should.eql('localhost');
            Should.not.exist(readConfig.port);
            readConfig.database.should.eql('optimage');
            readConfig.user.should.eql('optimagedev');
            readConfig.password.should.eql('=7QM^!4ynHebT7s7');
            readConfig.connectionLimit.should.eql(10);
        });

        it('Should load from config-2/test.json', function() {
            const nodeConfig = Internals.resetNodeConfig('config-2');
            const readConfig = MysqlPoolConnectionsConfigFactory.createReadConnectionConfigFromNodeConfig(nodeConfig);

            Should.not.exist(readConfig);
        });

    });

});

class Internals {

    /**
     * @param {string} configDirName
     */
    static resetNodeConfig(configDirName) {
        process.env.NODE_CONFIG_DIR = Path.resolve(__dirname, '../unit-helper/mysqlPoolConnectionConfigFactory', configDirName);
        return require('config-uncached')(true);
    }

}