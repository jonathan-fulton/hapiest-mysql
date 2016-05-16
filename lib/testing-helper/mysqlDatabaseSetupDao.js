'use strict';

const Promise = require('bluebird');
const Async = require('async');

class MysqlDatabaseSetupDao {

    /**
     * @param {MysqlService} mysqlService
     * @param {string} schemaSourceDatabase
     */
    constructor(mysqlService, schemaSourceDatabase) {
        this._mysqlService = mysqlService;
        this._schemaSourceDatabase = schemaSourceDatabase;
    }

    /**
     * @returns {Promise}
     */
    replicateAllTables() {
        return this._getAllTablesFromSourceDatabase()
            .then(tables => {
                return Promise.all([this.replicateTables(tables)]);
            });
    }

    /**
     * @param {string[]} tables
     * @returns {Promise}
     */
    replicateTables(tables) {
        return Promise.resolve()
            .then(() => this._dropTables(tables))
            .then(() => this._createTables(tables))
            ;
    }

    copyDataForAllTables() {
        return this._getAllTablesFromSourceDatabase()
            .then(tables => {
               return Promise.all([this.copyDataForTables(tables)]);
            });
    }

    copyDataForTables(tables) {
        const copyDataSql = this._copyDataSql(tables);
        return Promise.all([this._mysqlService.executeGenericQuery(copyDataSql)]).then(results => results[0]);
    }

    /**
     * @returns {Promise.<String[]>} - an array of table names
     */
    _getAllTablesFromSourceDatabase() {
        const allTablesQuery = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${this._schemaSourceDatabase}';`;
        return this._mysqlService.selectAll(allTablesQuery)
            .then(allTables => allTables.map(tableRow => tableRow.TABLE_NAME));
    }

    /**
     * @param {string[]} tables
     * @returns {Promise.<*>}
     */
    _dropTables(tables) {
        const dropTablesSql = this._getDropTablesSql(tables);
        return Promise.all([this._mysqlService.executeGenericQuery(dropTablesSql)]).then(results => results[0]);
    }

    /**
     * @param {string[]} tables
     * @returns {string}
     */
    _getDropTablesSql(tables) {
        const dropTableQueries = ['SET foreign_key_checks = 0;'];
        tables.forEach(table => dropTableQueries.push(`DROP TABLE IF EXISTS ${table};`));
        dropTableQueries.push('SET foreign_key_checks = 1;');

        return dropTableQueries.join("\n");
    }

    _createTables(tables) {
        return this._getCreateTablesSql(tables)
                .then(createTablesSql => {
                    return Promise.all([this._mysqlService.executeGenericQuery(createTablesSql)])
                        .then(results => results[0])
                })
        ;
    }

    /**
     * @param {string[]} tables
     * @returns {Promise.<string>}
     */
    _getCreateTablesSql(tables) {
        const createTablesQueries = ['SET foreign_key_checks = 0;'];
        const self = this;

        return Promise.mapSeries(tables, (table) => {
                return self._getCreateTableSql(table)
                    .then((sql) => {
                        createTablesQueries.push(sql);
                    });
            })
            .then(() => {
                createTablesQueries.push('SET foreign_key_checks = 1;');
                return createTablesQueries.join("\n")
            })
        ;
    }

    /**
     * @param {string} table
     * @returns {Promise.<string>}
     */
    _getCreateTableSql(table) {
        const getCreateTableSql = `SHOW CREATE TABLE ${this._schemaSourceDatabase}.${table};`;
        return this._mysqlService.executeGenericQuery(getCreateTableSql)
            .then(createTableResult => createTableResult[0]['Create Table']+';')
        ;
    }

    /**
     * @param tables
     * @private
     */
    _copyDataSql(tables) {
        const copyDataQueries = ['SET foreign_key_checks = 0;'];
        tables.forEach(table => copyDataQueries.push(`INSERT INTO ${table} SELECT * FROM ${this._schemaSourceDatabase}.${table};`));
        copyDataQueries.push('SET foreign_key_checks = 1;')
        return copyDataQueries.join("\n");
    }
}

module.exports = MysqlDatabaseSetupDao;
