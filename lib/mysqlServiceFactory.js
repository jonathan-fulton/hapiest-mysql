'use strict';

const MysqlPoolConnectionConfigFactory = require('./mysqlPoolConnectionConfigFactory');
const MysqlService = require('./mysqlService');

// @TODO: add a nodeConfig "root" concept so createFromNodeConfig can apply for multiple database connections within a config file

class MysqlServiceFactory {

    /**
     * @param {Config} nodeConfig
     * @param {Logger} [logger] - instance of hapiest-logger
     *
     * @returns {MysqlService}
     */
    static createFromNodeConfig(nodeConfig, logger) {
        const writeConnectionConfig = MysqlPoolConnectionConfigFactory.createWriteConnectionConfigFromNodeConfig(nodeConfig);
        const readConnectionConfig = MysqlPoolConnectionConfigFactory.createReadConnectionConfigFromNodeConfig(nodeConfig);

        return MysqlServiceFactory.create(writeConnectionConfig, readConnectionConfig, logger);
    }

    /**
     * @param {object} connectionConfig
     * @param {Logger} [logger] - instance of hapiest-logger
     *
     * @returns {MysqlService}
     */
    static createFromObjWithOnePool(connectionConfig, logger) {
        const writeConnectionConfig = MysqlPoolConnectionConfigFactory.createConnectionConfig(connectionConfig);
        const readConnectionConfig = null;
        return MysqlServiceFactory.create(writeConnectionConfig, readConnectionConfig, logger);
    }

    /**
     * @param {object} writeConnectionConfigObj
     * @param {object} readConnectionConfigObj
     * @param {Logger} [logger] - instance of hapiest-logger
     *
     * @returns {MysqlService}
     */
    static createFromObj(writeConnectionConfigObj, readConnectionConfigObj, logger) {
        const writeConnectionConfig = MysqlPoolConnectionConfigFactory.createConnectionConfig(writeConnectionConfigObj);
        const readConnectionConfig = MysqlPoolConnectionConfigFactory.createConnectionConfig(readConnectionConfigObj);
        return MysqlServiceFactory.create(writeConnectionConfig, readConnectionConfig, logger);
    }

    /**
     * @param {MysqlPoolConnectionConfig} writeConnectionConfig
     * @param {MysqlPoolConnectionConfig} readConnectionConfig
     * @param {Logger} [logger] - instance of hapiest-logger
     *
     * @returns {MysqlService}
     */
    static create(writeConnectionConfig, readConnectionConfig, logger) {
        return new MysqlService(writeConnectionConfig, readConnectionConfig, logger);
    }
    
}

module.exports = MysqlServiceFactory;