'use strict';

class MysqlDatabaseSetup {

    /**
     * @param {MysqlDatabaseSetupDao} dao
     */
    constructor(dao) {
        this._dao = dao;
    }

    /**
     * @returns {Promise}
     */
    replicateAllTables() {
        return this._dao.replicateAllTables();
    }

    /**
     * @param {string[]} tables - list of tables to recreate in our unit testing database
     * @returns {Promise}
     */
    replicateTables(tables) {
        return this._dao.replicateTables(tables);
    }

    /**
     * @returns {Promise}
     */
    copyDataForAllTables() {
        return this._dao.copyDataForAllTables();
    }

    /**
     * @param {string[]} tables
     * @returns {Promise}
     */
    copyDataForTables(tables) {
        return this._dao.copyDataForTables(tables);
    }

    /**
     * @returns {Promise}
     */
    dropAllTables() {
        return this._dao.dropAllTables();
    }

    /**
     * @param {string[]} tables
     * @returns {Promise}
     */
    dropTables(tables) {
        return this._dao.dropTables(tables);
    }

    /**
     * @returns {Promise.<String[]>}
     */
    getAllTablesFromSourceDatabase() {
        return this._dao.getAllTablesFromSourceDatabase();
    }

    /**
     * @returns {Promise.<String[]>}
     */
    getAllTablesFromCurrentDatabase() {
        return this._dao.getAllTablesFromCurrentDatabase();
    }

}

module.exports = MysqlDatabaseSetup;