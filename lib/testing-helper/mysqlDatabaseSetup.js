'use strict';

class MysqlDatabaseSetup {

    /**
     * @param {MysqlDatabaseSetupDao} dao
     */
    constructor(dao) {
        this._dao = dao;
    }

    /**
     *
     */
    replicateAllTables() {
        return this._dao.replicateAllTables();
    }

    /**
     * @param {string[]} tables - list of tables to recreate in our unit testing database
     */
    replicateTables(tables) {
        return this._dao.replicateTables(tables);
    }

    copyDataForAllTables() {
        return this._dao.copyDataForAllTables();
    }

    copyDataForTables(tables) {
        return this._dao.copyDataForTables(tables);
    }

}

module.exports = MysqlDatabaseSetup;