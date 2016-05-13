'use strict';

const MysqlPoolConnectionConfigFactory = require('./mysqlPoolConnectionConfigFactory');
const MysqlService = require('./mysqlService');

class MysqlServiceFactory {

    /**
     * @param {Config} nodeConfig
     *
     * @returns {MysqlService}
     */
    static createFromNodeConfig(nodeConfig) {
        const writeConnectionConfig = MysqlPoolConnectionConfigFactory.createWriteConnectionConfigFromNodeConfig(nodeConfig);
        const readConnectionConfig = MysqlPoolConnectionConfigFactory.createReadConnectionConfigFromNodeConfig(nodeConfig);

        return MysqlServiceFactory.create(writeConnectionConfig, readConnectionConfig);
    }

    /**
     * @param {object} connectionConfig
     *
     * @returns {MysqlService}
     */
    static createFromObjWithOnePool(connectionConfig) {
        const writeConnectionConfig = MysqlPoolConnectionConfigFactory.createConnectionConfig(connectionConfig);
        return new MysqlService(writeConnectionConfig);
    }

    /**
     * @param {object} writeConnectionConfigObj
     * @param {object} readConnectionConfigObj
     *
     * @returns {MysqlService}
     */
    static createFromObj(writeConnectionConfigObj, readConnectionConfigObj) {
        const writeConnectionConfig = MysqlPoolConnectionConfigFactory.createConnectionConfig(writeConnectionConfigObj);
        const readConnectionConfig = MysqlPoolConnectionConfigFactory.createConnectionConfig(readConnectionConfigObj);
        return MysqlServiceFactory.create(writeConnectionConfig, readConnectionConfig);
    }

    /**
     * @param {MysqlPoolConnectionConfig} writeConnectionConfig
     * @param {MysqlPoolConnectionConfig} readConnectionConfig
     *
     * @returns {MysqlService}
     */
    static create(writeConnectionConfig, readConnectionConfig) {
        return new MysqlService(writeConnectionConfig, readConnectionConfig);
    }
    
}

module.exports = MysqlServiceFactory;