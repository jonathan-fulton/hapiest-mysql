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
     * @param {boolean} resetAutoIncrement
     * @returns {Promise}
     */
    replicateAllTables(resetAutoIncrement) {
        return this.getAllTablesFromSourceDatabase()
            .then(tables => {
                return Promise.all([this.replicateTables(tables, resetAutoIncrement)]);
            });
    }

    /**
     * @param {string[]} tables
     * @param {boolean} resetAutoIncrement
     * @returns {Promise}
     */
    replicateTables(tables, resetAutoIncrement) {
        return this.dropTables(tables)
                .then(() => Promise.all([this._createTables(tables, resetAutoIncrement)]));
    }

    copyDataForAllTables() {
        return this.getAllTablesFromSourceDatabase()
            .then(tables => {
               return Promise.all([this.copyDataForTables(tables)]);
            });
    }

    copyDataForTables(tables) {
        const copyDataSql = this._copyDataSql(tables);
        return Promise.all([this._mysqlService.executeGenericQuery(copyDataSql)]).then(results => results[0]);
    }

    /**
     * @param {string[]} tables
     * @returns {Promise.<*>}
     */
    dropTables(tables) {
        const dropTablesSql = this._getDropTablesSql(tables);
        return Promise.all([this._mysqlService.executeGenericQuery(dropTablesSql)]).then(results => results[0]);
    }

    /**
     * @returns {Promise.<*>}
     */
    dropAllTables() {
        return this.getAllTablesFromCurrentDatabase()
            .then(tables => {
                return Promise.all([this.dropTables(tables)]);
            });
    }

    /**
     * @returns {Promise.<String[]>}
     */
    getAllTablesFromSourceDatabase() {
        return this._getAllTables(this._schemaSourceDatabase);
    }


    /**
     * @returns {Promise.<String[]>}
     */
    getAllTablesFromCurrentDatabase() {
        return this._getAllTables();
    }

    /**
     * @param {string} [database] - null fetches tables from the current database returned by DATABASE()
     * @returns {Promise.<String[]>}
     * @private
     */
    _getAllTables(database) {
        if (database) {
            database = `'${database}'`;
        } else {
            database = 'DATABASE()';
        }

        const allTablesQuery = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ${database};`;
        return this._mysqlService.selectAll(allTablesQuery)
            .then(allTables => allTables.map(tableRow => tableRow.TABLE_NAME));
    }

    /**
     * @param {string[]} tables
     * @returns {string}
     * @private
     */
    _getDropTablesSql(tables) {
        const dropTableQueries = ['SET foreign_key_checks = 0;'];
        tables.forEach(table => dropTableQueries.push(`DROP TABLE IF EXISTS ${table};`));
        dropTableQueries.push('SET foreign_key_checks = 1;');

        return dropTableQueries.join("\n");
    }

    /**
     * @param {Array.<String>} tables
     * @param {boolean} resetAutoIncrement
     * @returns {Promise}
     * @private
     */
    _createTables(tables, resetAutoIncrement) {
        return this._getCreateTablesSql(tables, resetAutoIncrement)
                .then(createTablesSql => {
                    return this._mysqlService.executeGenericQuery(createTablesSql);
                })
        ;
    }

    /**
     * @param {string[]} tables
     * @param {boolean} resetAutoIncrement
     * @returns {Promise.<string>}
     * @private
     */
    _getCreateTablesSql(tables, resetAutoIncrement) {
        const createTablesQueries = ['SET foreign_key_checks = 0;'];
        const self = this;

        return Promise.mapSeries(tables, (table) => {
                return self._getCreateTableSql(table)
                    .then((sql) => {
                        createTablesQueries.push(sql);

                        if (resetAutoIncrement) {
                            createTablesQueries.push(self._getResetAutoIncrement(table));
                        }
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
     * @private
     */
    _getCreateTableSql(table) {
        const getCreateTableSql = `SHOW CREATE TABLE ${this._schemaSourceDatabase}.${table};`;
        return this._mysqlService.executeGenericQuery(getCreateTableSql)
            .then(createTableResult => createTableResult[0]['Create Table']+';')
        ;
    }

    /**
     * @param {string} table
     * @returns {string}
     * @private
     */
    _getResetAutoIncrement(table) {
        return `ALTER TABLE ${table} AUTO_INCREMENT=1;`;
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
