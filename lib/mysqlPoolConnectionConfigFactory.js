'use strict';

const NodeConfig = require('config');

const MysqlPoolConnectionConfig = require('./mysqlPoolConnectionConfig');

class MysqlPoolConnectionsConfigFactory {

    static createWriteConnectionConfigFromNodeConfig() {
        const config = Internals.createConnectionConfig('write');
        return MysqlPoolConnectionsConfigFactory.createConnectionConfig(config);
    }

    static createReadConnectionConfigFromNodeConfig() {
        // Check that the read config exists - requires a NULL override so we have to check the value is non-null if it exists
        if (NodeConfig.has('database.read') && NodeConfig.get('database.read')) {
            const config = Internals.createConnectionConfig('read');
            return MysqlPoolConnectionsConfigFactory.createConnectionConfig(config);
        } else {
            return null;
        }
    }

    static createConnectionConfig(config) {
        return new MysqlPoolConnectionConfig(config);
    }

}

module.exports = MysqlPoolConnectionsConfigFactory;

class Internals {
    static createConnectionConfig(writeOrRead) {
        const config = {
            host: Internals.getValue(writeOrRead, 'host'),
            user: Internals.getValue(writeOrRead, 'user'),
            password: Internals.getValue(writeOrRead, 'password'),
            database: Internals.getValue(writeOrRead, 'database'),
            connectionLimit: Internals.getValue(writeOrRead, 'connectionLimit')
        };

        Internals.mergeIfDefined(config, writeOrRead, 'port');

        return config;
    }

    static getValue(writeOrRead, propertyName) {
        return NodeConfig.get('database.' + writeOrRead + '.' + propertyName);
    }

    static mergeIfDefined(config, writeOrRead, propertyName) {
        if (NodeConfig.has('database.' + writeOrRead + '.' + propertyName)) {
            config[propertyName] = Internals.getValue(writeOrRead, propertyName);
        }
    }
}