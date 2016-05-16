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

    replicateAllTables(done) {
        const self = this;

        Async.auto({
            tables: (next) => {
                self.getAllTablesFromSourceDatabase(next)
            },
            dropTables: ['tables', function(results, next) {
                const tables = results.tables;
                const dropTablesSql = self.getDropTablesSql(tables);

                self._mysqlService.executeGenericQuery(dropTablesSql)
                    .then(() => next());
            }],
            createTables: ['dropTables', function(results, next) {
                const tables = results.tables;
                self.getCreateTablesSql(tables, (err, sql) => {
                    self._mysqlService.executeGenericQuery(sql)
                        .then(() => next());
                })
            }]
        }, (err, results) => {
            done();
        });
    }

    /**
     * @returns {Promise.<String[]>} - an array of table names
     */
    getAllTablesFromSourceDatabase(done) {
        const allTablesQuery = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${this._schemaSourceDatabase}';`;
        return this._mysqlService.selectAll(allTablesQuery)
            .then(allTables => {
                done(null, allTables.map(tableRow => tableRow.TABLE_NAME))
            });
    }

    /**
     * @param tables
     * @returns {string}
     */
    getDropTablesSql(tables) {
        const dropTableQueries = ['SET foreign_key_checks = 0;'];
        tables.forEach(table => dropTableQueries.push(`DROP TABLE IF EXISTS ${table};`));
        dropTableQueries.push('SET foreign_key_checks = 1;');

        return dropTableQueries.join("\n");
    }

    /**
     * @param {String[]} tables
     * @param {function(err, string)} done
     */
    getCreateTablesSql(tables, done) {
        const createTablesQueries = ['SET foreign_key_checks = 0;'];
        const self = this;

        Async.eachSeries(tables, (table, eachNext) => {
            self.getCreateTableSql(table, (err, sql) => {
                createTablesQueries.push(sql);
                eachNext();
            })
        }, () => {
            createTablesQueries.push('SET foreign_key_checks = 1;');
            done(null, createTablesQueries.join("\n"));
        });
    }

    getCreateTableSql(table, done) {
        this.getCreateTableStatement(table, (err, sql) => {
            done(null, sql);
        });
    }

    getCreateTableStatement(table, done) {
        const getCreateTableSql = `SHOW CREATE TABLE ${this._schemaSourceDatabase}.${table};`;
        this._mysqlService.executeGenericQuery(getCreateTableSql)
            .then(createTableResult => done(null, createTableResult[0]['Create Table'] + ';'))
        ;
    }

}

module.exports = MysqlDatabaseSetupDao;
