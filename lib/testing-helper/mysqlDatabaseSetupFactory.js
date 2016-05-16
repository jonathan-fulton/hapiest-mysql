'use strict';

const MysqlDatabaseSetup = require('./mysqlDatabaseSetup');
const MysqlDatabaseSetupDaoFactory = require('./mysqlDatabaseSetupDaoFactory');

class MysqlDatabaseSetupFactory {

    /**
     * @param {MysqlService} mysqlService
     * @param {string} schemaSourceDatabase
     * @returns {MysqlDatabaseSetup}
     */
    static create(mysqlService, schemaSourceDatabase) {
        const dao = MysqlDatabaseSetupDaoFactory.create(mysqlService, schemaSourceDatabase);
        return new MysqlDatabaseSetup(dao);
    }

}

module.exports = MysqlDatabaseSetupFactory;