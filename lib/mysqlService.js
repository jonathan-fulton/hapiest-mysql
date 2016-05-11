'use strict';

const Mysql = require('mysql');
const Promise = require('bluebird');
const MysqlQuery = require('./mysqlQuery');
const MysqlInsertResultFactory = require('./mysqlModificationResultFactory');

class MysqlService {

    /**
     * @param {MysqlPoolConnectionConfig} writeConnectionConfig
     * @param {MysqlPoolConnectionConfig} [readConnectionConfig] - note, if you leave this NULL then reads will use the write connection
     */
    constructor(writeConnectionConfig, readConnectionConfig) {
        this._writePool = Promise.promisifyAll(Mysql.createPool(writeConnectionConfig));

        // Read from the write DB if a separate connection isn't provided for reading
        this._readPool = (readConnectionConfig) ? Promise.promisifyAll(Mysql.createPool(readConnectionConfig)) : this._writePool;
    }

    /**
     * @param {string} selectQuery
     * @returns {Promise.<object|null,Error>}
     */
    selectOne(selectQuery) {
        const selectOneResult = true;
        return Internals.selectBase(this._readPool, selectQuery, selectOneResult);
    }

    /**
     * @param {string} selectQuery
     * @returns {Promise.<object|null,Error>}
     */
    selectOneFromMaster(selectQuery) {
        const selectOneResult = true;
        return Internals.selectBase(this._writePool, selectQuery, selectOneResult);
    }

    /**
     * @param {string} selectQuery
     * @returns {Promise.<object[]|null,Error>}
     */
    selectAll(selectQuery) {
        const selectOneResult = false;
        return Internals.selectBase(this._readPool, selectQuery, selectOneResult);
    }

    /**
     * @param {string} selectQuery
     * @returns {Promise.<object[]|null,Error>}
     */
    selectAllFromMaster(selectQuery) {
        const selectOneResult = false;
        return Internals.selectBase(this._writePool, selectQuery, selectOneResult);
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
        ;
    }

    delete(deleteQuery) {
        return Promise.resolve()
            .then(() => MysqlQuery.validateDelete(deleteQuery))
            .then(() => this._writePool.queryAsync(deleteQuery))
            .then((result) => Promise.resolve(MysqlInsertResultFactory.createFromResult(result)))
        ;
    }

    executeGenericQuery(query) {
        return Promise.resolve()
            .then(() => this._writePool.queryAsync(query)) // We have to execute generic queries against "master");
        ;
    }

    clean(input) {
        return Mysql.escape(input);
    }

}

module.exports = MysqlService;

class Internals {
    /**
     * @param pool
     * @param selectQuery
     * @param returnOneRow
     * @returns {Promise.<object|null>}
     */
    static selectBase(pool, selectQuery, returnOneRow) {
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
            });
    }
}