'use strict';

const MysqlPoolConnectionConfigFactory = require('./mysqlPoolConnectionConfigFactory');
const MysqlService = require('./mysqlService');

class MysqlServiceFactory {

    /**
     * @param {Config} nodeConfig
     * @param {Logger} [logger] - instance of hapiest-logger
     * @param {string} [nodeConfigKey='database'] nodeConfigParam
     *
     * @returns {MysqlService}
     */
    static createFromNodeConfig(nodeConfig, logger, nodeConfigKey) {
        nodeConfigKey = nodeConfigKey || 'database';

        const writeConnectionConfig = MysqlPoolConnectionConfigFactory.createWriteConnectionConfigFromNodeConfig(nodeConfig, nodeConfigKey);
        const readConnectionConfig = MysqlPoolConnectionConfigFactory.createReadConnectionConfigFromNodeConfig(nodeConfig, nodeConfigKey);

        let options;
        const optionsKey = `${nodeConfigKey}.options`;
        if (nodeConfig.has(optionsKey)) {
            options = nodeConfig.get(optionsKey);
        }

        return MysqlServiceFactory.create(writeConnectionConfig, readConnectionConfig, logger, options);
    }

    /**
     * @param {object} connectionConfig
     * @param {Logger} [logger] - instance of hapiest-logger
     * @param {MysqlServiceOptions} options
     *
     * @returns {MysqlService}
     */
    static createFromObjWithOnePool(connectionConfig, logger, options) {
        const writeConnectionConfig = MysqlPoolConnectionConfigFactory.createConnectionConfig(connectionConfig);
        const readConnectionConfig = null;
        return MysqlServiceFactory.create(writeConnectionConfig, readConnectionConfig, logger, options);
    }

    /**
     * @param {object} writeConnectionConfigObj
     * @param {object} readConnectionConfigObj
     * @param {Logger} [logger] - instance of hapiest-logger
     * @param {MysqlServiceOptions} options
     *
     * @returns {MysqlService}
     */
    static createFromObj(writeConnectionConfigObj, readConnectionConfigObj, logger, options) {
        const writeConnectionConfig = MysqlPoolConnectionConfigFactory.createConnectionConfig(writeConnectionConfigObj);
        const readConnectionConfig = MysqlPoolConnectionConfigFactory.createConnectionConfig(readConnectionConfigObj);
        return MysqlServiceFactory.create(writeConnectionConfig, readConnectionConfig, logger, options);
    }

    /**
     * @param {MysqlPoolConnectionConfig} writeConnectionConfig
     * @param {MysqlPoolConnectionConfig} readConnectionConfig
     * @param {Logger} [logger] - instance of hapiest-logger
     * @param {MysqlServiceOptions} [options]
     *
     * @returns {MysqlService}
     */
    static create(writeConnectionConfig, readConnectionConfig, logger, options) {
        return new MysqlService(writeConnectionConfig, readConnectionConfig, logger, options);
    }
    
}

module.exports = MysqlServiceFactory;