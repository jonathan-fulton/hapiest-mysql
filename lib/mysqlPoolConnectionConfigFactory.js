'use strict';

const MysqlPoolConnectionConfig = require('./mysqlPoolConnectionConfig');

class MysqlPoolConnectionsConfigFactory {

    /**
     * @param {Config} nodeConfig
     * @param {string} [nodeConfigKey='database']
     * 
     * @returns {MysqlPoolConnectionConfig}
     */
    static createWriteConnectionConfigFromNodeConfig(nodeConfig, nodeConfigKey) {
        nodeConfigKey = nodeConfigKey || 'database';

        const config = Internals.createConnectionConfig(nodeConfig, `${nodeConfigKey}.write`);
        return MysqlPoolConnectionsConfigFactory.createConnectionConfig(config);
    }

    /**
     * @param {Config} nodeConfig
     * @param {string} [nodeConfigKey='database']
     *
     * @returns {MysqlPoolConnectionConfig}
     */
    static createReadConnectionConfigFromNodeConfig(nodeConfig, nodeConfigKey) {
        nodeConfigKey = nodeConfigKey || 'database';
        const readNodeConfigKey = `${nodeConfigKey}.read`;

        // Check that the read config exists - requires a NULL override so we have to check the value is non-null if it exists
        if (nodeConfig.has(readNodeConfigKey) && nodeConfig.get(readNodeConfigKey)) {
            const config = Internals.createConnectionConfig(nodeConfig, readNodeConfigKey);
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
     * @param {string} nodeConfigKey - e.g., database.write or database.read
     * @returns {{host: *, user: *, password: *, database: *, connectionLimit: *}}
     */
    static createConnectionConfig(nodeConfig, nodeConfigKey) {
        const config = {
            host: Internals.getValue(nodeConfig, nodeConfigKey, 'host'),
            user: Internals.getValue(nodeConfig, nodeConfigKey, 'user'),
            password: Internals.getValue(nodeConfig, nodeConfigKey, 'password'),
            database: Internals.getValue(nodeConfig, nodeConfigKey, 'database'),
            connectionLimit: Internals.getValue(nodeConfig, nodeConfigKey, 'connectionLimit')
        };

        Internals.mergeIfDefined(nodeConfig, config, nodeConfigKey, 'port');
        Internals.mergeIfDefined(nodeConfig, config, nodeConfigKey, 'multipleStatements');
        Internals.mergeIfDefined(nodeConfig, config, nodeConfigKey, 'timezone');

        return config;
    }

    static getValue(nodeConfig, nodeConfigKey, propertyName) {
        return nodeConfig.get(`${nodeConfigKey}.${propertyName}`);
    }

    static mergeIfDefined(nodeConfig, config, nodeConfigKey, propertyName) {
        if (nodeConfig.has(`${nodeConfigKey}.${propertyName}`)) {
            config[propertyName] = Internals.getValue(nodeConfig, nodeConfigKey, propertyName);
        }
    }
}