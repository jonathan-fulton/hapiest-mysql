'use strict';

const MysqlDatabaseSetupDao = require('./mysqlDatabaseSetupDao');

class MysqlDatabaseSetupDaoFactory {

    /**
     * @param {MysqlService} mysqlService
     * @param {string} schemaSourceDatabase
     * @returns {MysqlDatabaseSetupDao}
     */
    static create(mysqlService, schemaSourceDatabase) {
        return new MysqlDatabaseSetupDao(mysqlService, schemaSourceDatabase);
    }

}

module.exports = MysqlDatabaseSetupDaoFactory;