'use strict';

const MysqlPoolConnectionConfig = require('./mysqlPoolConnectionConfig');

class MysqlPoolConnectionsConfigFactory {

    /**
     * @param {Config} nodeConfig
     * 
     * @returns {MysqlPoolConnectionConfig}
     */
    static createWriteConnectionConfigFromNodeConfig(nodeConfig) {
        const config = Internals.createConnectionConfig(nodeConfig, 'write');
        return MysqlPoolConnectionsConfigFactory.createConnectionConfig(config);
    }

    /**
     * @param {Config} nodeConfig
     *
     * @returns {MysqlPoolConnectionConfig}
     */
    static createReadConnectionConfigFromNodeConfig(nodeConfig) {
        // Check that the read config exists - requires a NULL override so we have to check the value is non-null if it exists
        if (nodeConfig.has('database.read') && nodeConfig.get('database.read')) {
            const config = Internals.createConnectionConfig(nodeConfig, 'read');
            return MysqlPoolConnectionsConfigFactory.createConnectionConfig(config);
        } else {
            return null;
        }
    }

    /**
     * @param {object} config
     * @param {string} config.host
     * @param {int} [config.port]
     * @param {string} config.database
     * @param {string} config.user
     * @param {string} config.password
     * @param {int} config.connectionLimit
     * @param {bool} [config.multipleStatements]
     *
     * @returns {MysqlPoolConnectionConfig}
     */
    static createConnectionConfig(config) {
        return new MysqlPoolConnectionConfig(config);
    }

}

module.exports = MysqlPoolConnectionsConfigFactory;

class Internals {
    /**
     * @param {Config} nodeConfig
     * @param {'write'|'read'} writeOrRead
     * @returns {{host: *, user: *, password: *, database: *, connectionLimit: *}}
     */
    static createConnectionConfig(nodeConfig, writeOrRead) {
        const config = {
            host: Internals.getValue(nodeConfig, writeOrRead, 'host'),
            user: Internals.getValue(nodeConfig, writeOrRead, 'user'),
            password: Internals.getValue(nodeConfig, writeOrRead, 'password'),
            database: Internals.getValue(nodeConfig, writeOrRead, 'database'),
            connectionLimit: Internals.getValue(nodeConfig, writeOrRead, 'connectionLimit')
        };

        Internals.mergeIfDefined(nodeConfig, config, writeOrRead, 'port');
        Internals.mergeIfDefined(nodeConfig, config, writeOrRead, 'multipleStatements');

        return config;
    }

    static getValue(nodeConfig, writeOrRead, propertyName) {
        return nodeConfig.get('database.' + writeOrRead + '.' + propertyName);
    }

    static mergeIfDefined(nodeConfig, config, writeOrRead, propertyName) {
        if (nodeConfig.has('database.' + writeOrRead + '.' + propertyName)) {
            config[propertyName] = Internals.getValue(nodeConfig, writeOrRead, propertyName);
        }
    }
}