'use strict';

const Mysql = require('mysql');
const Promise = require('bluebird');
const Chance = require('chance');
const _ = require('lodash');
const MysqlQuery = require('./mysqlQuery');
const MysqlInsertResultFactory = require('./mysqlModificationResultFactory');

class MysqlService {

    /**
     * @param {MysqlPoolConnectionConfig} writeConnectionConfig
     * @param {MysqlPoolConnectionConfig} [readConnectionConfig] - note, if you leave this NULL then reads will use the write connection
     * @param {Logger} [logger] - instance of hapiest-logger
     */
    constructor(writeConnectionConfig, readConnectionConfig, logger) {
        this._writePool = Promise.promisifyAll(Mysql.createPool(writeConnectionConfig));

        if (readConnectionConfig) {
            const readConnectionConfigs = this._expandMysqlPoolConnectionConfig(readConnectionConfig);
            this._readPools = readConnectionConfigs.map(readConfig => Promise.promisifyAll(Mysql.createPool(readConfig)));
        } else {
            // Read from the write DB if a separate connection isn't provided for reading
            this._readPools = [this._writePool];
        }

        this._logger = logger; // @TODO: throw an error if logger is not of correct type
        this._chance = new Chance();
    }

    /**
     * Converts input to an array, creating one output for every host input (facilitates multiple read slaves)
     * @param {MysqlPoolConnectionConfig} mysqlPoolConnectionConfig
     * @returns {MysqlPoolConnectionConfig[]}
     * @private
     */
    _expandMysqlPoolConnectionConfig(mysqlPoolConnectionConfig) {
        const expandedConnectionConfigs = [];
        if (Array.isArray(mysqlPoolConnectionConfig.host)) {
            const baseConfig = mysqlPoolConnectionConfig.toJSON();
            mysqlPoolConnectionConfig.host.forEach(host => {
                const config = _.clone(baseConfig);
                config.host = host;
                expandedConnectionConfigs.push(config);
            })

        } else {
            expandedConnectionConfigs.push(mysqlPoolConnectionConfig);
        }
        return expandedConnectionConfigs;
    }

    _getReadPool() {
        const slaveIndex = this._chance.integer({min:0, max:this._readPools.length-1});
        return this._readPools[slaveIndex];
    }

    /**
     * @param {string} selectQuery
     * @returns {Promise.<object|null,Error>}
     */
    selectOne(selectQuery) {
        const selectOneResult = true;
        return this._selectBase(this._getReadPool(), selectQuery, selectOneResult);
    }

    /**
     * @param {string} selectQuery
     * @returns {Promise.<object|null,Error>}
     */
    selectOneFromMaster(selectQuery) {
        const selectOneResult = true;
        return this._selectBase(this._writePool, selectQuery, selectOneResult);
    }

    /**
     * @param {string} selectQuery
     * @returns {Promise.<object[]|null,Error>}
     */
    selectAll(selectQuery) {
        const selectOneResult = false;
        return this._selectBase(this._getReadPool(), selectQuery, selectOneResult);
    }

    /**
     * @param {string} selectQuery
     * @returns {Promise.<object[]|null,Error>}
     */
    selectAllFromMaster(selectQuery) {
        const selectOneResult = false;
        return this._selectBase(this._writePool, selectQuery, selectOneResult);
    }

    /**
     * @param pool
     * @param selectQuery
     * @param returnOneRow
     * @returns {Promise.<object|null>}
     */
    _selectBase(pool, selectQuery, returnOneRow) {
        return Promise.resolve()
            .tap(() => this._logSqlQuery(selectQuery))
            .then(() => MysqlQuery.validateSelect(selectQuery))
            .then(() => pool.queryAsync(selectQuery))
            .then((rows) => {
                if (rows.length === 0) {
                    const data = returnOneRow ? null : [];
                    return Promise.resolve(data);
                } else {
                    const data = returnOneRow ? rows[0] : rows;

                    // Regex from: http://weblog.west-wind.com/posts/2014/Jan/06/JavaScript-JSON-Date-Parsing-and-real-Dates
                    const dateRegEx = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*))(?:Z|(\+|-)([\d|:]*))?$/;
                    const jsObj = JSON.parse(JSON.stringify(data), function(key, value) { // Convert dates back to Date objects
                        if (typeof value === 'string') {
                            const match = dateRegEx.exec(value);
                            if (match) {
                                return new Date(value);
                            }
                        }
                        return value;
                    }); // using .parse.stringify to convert RowDataPacket --> standard JS objects
                    return Promise.resolve(jsObj);
                }
            })
            .catch(err => {
                this._logSqlError(err, selectQuery);
                throw err;
            });
    }

    /**
     * @param {string} insertQuery
     * @returns {Promise.<MysqlModificationResult|null,Error>}
     */
    insert(insertQuery) {
        return Promise.resolve()
            .tap(() => this._logSqlQuery(insertQuery))
            .then(() => MysqlQuery.validateInsert(insertQuery))
            .then(() => this._writePool.queryAsync(insertQuery))
            .then((result) => MysqlInsertResultFactory.createFromResult(result))
            .catch(err => {
                this._logSqlError(err, insertQuery);
                throw err;
            })
        ;
    }

    /**
     * @param {string} updateQuery
     * @returns {Promise.<MysqlModificationResult|null,Error>}
     */
    update(updateQuery) {
        return Promise.resolve()
            .tap(() => this._logSqlQuery(updateQuery))
            .then(() => MysqlQuery.validateUpdate(updateQuery))
            .then(() => this._writePool.queryAsync(updateQuery))
            .then((result) => MysqlInsertResultFactory.createFromResult(result))
            .catch(err => {
                this._logSqlError(err, updateQuery);
                throw err;
            })
        ;
    }

    /**
     * @param {string} deleteQuery
     * @returns {Promise.<MysqlModificationResult,Error>}
     */
    delete(deleteQuery) {
        return Promise.resolve()
            .tap(() => this._logSqlQuery(deleteQuery))
            .then(() => MysqlQuery.validateDelete(deleteQuery))
            .then(() => this._writePool.queryAsync(deleteQuery))
            .then((result) => MysqlInsertResultFactory.createFromResult(result))
            .catch(err => {
                this._logSqlError(err, deleteQuery);
                throw err;
            })
        ;
    }

    /**
     * @param {string} query
     * @returns {Promise.<*>}
     */
    executeGenericQuery(query) {
        return Promise.resolve()
            .tap(() => this._logSqlQuery(query))
            .then(() => {return this._writePool.queryAsync(query)}) // We have to execute generic queries against "master");
            .catch(err => {
                this._logSqlError(err, query);
                throw err;
            })
        ;
    }

    /**
     * Executes all queries using executeGenericQuery
     * @param {String[]} queries
     * @returns {Promise}
     */
    executeQueries(queries) {
        return Promise.mapSeries(queries, (query) => this.executeGenericQuery(query));
    }

    clean(input) {
        return Mysql.escape(input);
    }

    /**
     * @param {Error} err
     * @param {string} sql
     * @private
     */
    _logSqlError(err, sql) {
        if (this._logger) {
           this._logger.error('Error executing SQL', {sql: sql, err: err});
        }
    }

    _logSqlQuery(sql) {
        if (this._logger) {
            this._logger.debug('Executing SQL', {sql: sql})
        }
    }

}

module.exports = MysqlService;