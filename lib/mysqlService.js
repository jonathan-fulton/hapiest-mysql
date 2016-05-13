'use strict';

const Mysql = require('mysql');
const Promise = require('bluebird');
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

        // Read from the write DB if a separate connection isn't provided for reading
        this._readPool = (readConnectionConfig) ? Promise.promisifyAll(Mysql.createPool(readConnectionConfig)) : this._writePool;

        this._logger = logger; // @TODO: throw an error if logger is not of correct type
    }

    /**
     * @param {string} selectQuery
     * @returns {Promise.<object|null,Error>}
     */
    selectOne(selectQuery) {
        const selectOneResult = true;
        return this._selectBase(this._readPool, selectQuery, selectOneResult);
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
        return this._selectBase(this._readPool, selectQuery, selectOneResult);
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
            .then(() => MysqlQuery.validateSelect(selectQuery))
            .then(() => pool.queryAsync(selectQuery))
            .then((rows) => {
                if (rows.length === 0) {
                    const data = returnOneRow ? null : [];
                    return Promise.resolve(data);
                } else {
                    const data = returnOneRow ? rows[0] : rows;
                    const jsObj = JSON.parse(JSON.stringify(data)); // using .parse.stringify to convert RowDataPacket --> standard JS objects
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
            .then(() => MysqlQuery.validateInsert(insertQuery))
            .then(() => this._writePool.queryAsync(insertQuery))
            .then((result) => Promise.resolve(MysqlInsertResultFactory.createFromResult(result)))
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
            .then(() => MysqlQuery.validateUpdate(updateQuery))
            .then(() => this._writePool.queryAsync(updateQuery))
            .then((result) => Promise.resolve(MysqlInsertResultFactory.createFromResult(result)))
            .catch(err => {
                this._logSqlError(err, updateQuery);
                throw err;
            })
        ;
    }

    delete(deleteQuery) {
        return Promise.resolve()
            .then(() => MysqlQuery.validateDelete(deleteQuery))
            .then(() => this._writePool.queryAsync(deleteQuery))
            .then((result) => Promise.resolve(MysqlInsertResultFactory.createFromResult(result)))
            .catch(err => {
                this._logSqlError(err, deleteQuery);
                throw err;
            })
        ;
    }

    executeGenericQuery(query) {
        return Promise.resolve()
            .then(() => this._writePool.queryAsync(query)) // We have to execute generic queries against "master");
            .catch(err => {
                this._logSqlError(err, query);
                throw err;
            })
        ;
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

}

module.exports = MysqlService;