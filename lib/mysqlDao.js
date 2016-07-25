'use strict';

const _ = require('lodash');
const MysqlDaoQueryHelper = require('./mysqlDaoQueryHelper');

class MysqlDao {

    /**
     * @param {MysqlDaoArgs} args
     */
    constructor(args) {
        this._mysqlService = args.mysqlService;
        this._queryHelper = new MysqlDaoQueryHelper(this.tableName, args.mysqlService.clean.bind(args.mysqlService));
        this._createVoFromDbRow = args.createVoFromDbRowFunction;
        this._logger = args.logger;
    }

    /**
     * @returns {string}
     */
    get tableName() { throw new Error('Extending class must override tableName() with actual table') }

    /**
     * @returns {MysqlService}
     */
    get mysqlService() { return this._mysqlService; }

    /**
     * @param {object} createArgs
     * @returns {Promise.<int,Error>} - ID of last inserted item
     */
    create(createArgs) {
        const sql = this._queryHelper.create(createArgs);
        return this.createFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<int,Error>} - ID of last inserted item
     */
    createFromSql(sql) {
        return this._mysqlService.insert(sql)
            .then(result => result.insertId)
            .catch(err => {
                this._logger.error(err.message, {sql:sql, err:err});
                throw new Error(`MysqlDao.createFromSql() for ${this.tableName} failed`);
            });
    }

    /**
     * @param {Array.<object>} createArgsArr
     * @returns {Promise.<int,Error>} - number of affected rows
     */
    createBulk(createArgsArr) {
        const sql = this._queryHelper.createBulk(createArgsArr);
        return this.createBulkFromSql(sql);
    }

    createBulkFromSql(sql) {
        return this._mysqlService.insert(sql)
            .then(result => result.affectedRows)
            .catch(err => {
                this._logger.error(err.message, {sql:sql, err:err});
                throw new Error(`MysqlDao.createBulkFromSql() for ${this.tableName} failed`);
            });
    }

    /**
     * @param id
     * @returns {Promise.<object,Error>}
     */
    getOneById(id) {
        return this.getOne({id: id});
    }

    /**
     * @param {object} whereClause
     * @returns {Promise.<object|null,Error>}
     */
    getOne(whereClause) {
        const sql = this._queryHelper.getOne(whereClause);
        return this.getOneFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object|null, Error>}
     */
    getOneFromSqlRaw(sql) {
        return this.getOneFromSql(sql, {rawResults: true});
    }

    /**
     * @param {string} sql
     * @param {object} [options]
     * @param {boolean} [options.rawResults=false]
     * @returns {Promise.<object|null,Error>}
     */
    getOneFromSql(sql, options) {
        const defaultOptions = {
            rawResults: false
        };
        const optionsToUse = _.merge(defaultOptions, options);

        return this._mysqlService.selectOne(sql)
            .then(dbRow => {
                if (!dbRow) {
                    return null;
                }
                if (optionsToUse.rawResults) {
                    return dbRow;
                }
                return this._createVoFromDbRow(dbRow);
            })
            .catch(err => {
                this._logger.error(err.message, {sql: sql, err:err});
                throw new Error('MysqlDao.getOneFromSql() failed');
            });
    }

    /**
     * @param {object} whereClause
     * @returns {Promise.<object[],Error>}
     * 
     * @TODO: add an options field that allows order by, limit, arbitrary WHERE clause to AND / OR together
     */
    getAll(whereClause) {
        const sql = this._queryHelper.getAll(whereClause);
        return this.getAllFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object[], Error>}
     */
    getAllFromSqlRaw(sql) {
        return this.getAllFromSql(sql, {rawResults: true});
    }

    /**
     * @param {string} sql
     * @param {object} [options]
     * @param {boolean} [options.rawResults=false]
     * @returns {Promise.<object[],Error>}
     */
    getAllFromSql(sql, options) {
        const defaultOptions = {
            rawResults: false
        };
        const optionsToUse = _.merge(defaultOptions, options);

        return this._mysqlService.selectAll(sql)
            .then(dbRows => {
                if (optionsToUse.rawResults) {
                    return dbRows;
                } else {
                    const results = [];
                    dbRows.forEach(dbRow => results.push(this._createVoFromDbRow(dbRow)));
                    return results;
                }
            })
            .catch(err => {
                this._logger.error(err.message, {sql: sql, err:err});
                throw new Error('MysqlDao.getAllFromSql() failed');
            });
    }

    /**
     * @param {int} id
     * @param {object} updateArgs
     * @returns {Promise.<int>} - number of changed rows (should be 0 or 1)
     */
    updateById(id, updateArgs) {
        const whereClause = {id: id};
        return this.updateOne(whereClause, updateArgs);
    }

    /**
     * @param {object} whereClause
     * @param {object} updateArgs
     * @returns {Promise.<int>} - returns number of changed rows (should be 0 or 1)
     */
    updateOne(whereClause, updateArgs) {
        const sql = this._queryHelper.updateOne(whereClause, updateArgs);
        return this.updateFromSql(sql);
    }

    /**
     * @param {object} whereClause
     * @param {object} updateArgs
     * @returns {Promise.<int>}
     */
    updateMultiple(whereClause, updateArgs) {
        const sql = this._queryHelper.update(whereClause, updateArgs);
        return this.updateFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<int>} - returns number of changed rows (should be 0 or 1)
     */
    updateFromSql(sql) {
        return this._mysqlService.update(sql)
            .then(results => results.changedRows)
            .catch(err => {
                this._logger.error(err.message, {sql: sql, err:err});
                throw new Error('MysqlDao.updateFromSql() failed');
            });
    }

    /**
     * @param {int} id
     * @returns {Promise.<int>} - number of affected rows (should be 0 or 1)
     */
    deleteById(id) {
        return this.deleteOne({id: id});
    }

    /**
     * @param {object} whereClause
     * @returns {Promise.<int>} - number of affected rows (should be 0 or 1)
     */
    deleteOne(whereClause) {
        const sql = this._queryHelper.deleteOne(whereClause);
        return this.deleteFromSql(sql);
    }

    /**
     * @param {object} whereClause
     * @returns {Promise.<int>}
     */
    deleteMultiple(whereClause) {
        const sql = this._queryHelper.delete(whereClause);
        return this.deleteFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<int>} - number of affected rows (should be 0 or 1)
     */
    deleteFromSql(sql) {
        return this._mysqlService.delete(sql)
            .then(results => results.affectedRows)
            .catch(err => {
                this._logger.error(err.message, {sql: sql, err:err});
                throw new Error('MysqlDao.deleteFromSql() failed');
            });
    }

    /**
     * @param uncleanValue
     * @returns {string|number}
     */
    clean(uncleanValue) { return this._queryHelper.clean(uncleanValue); }

    /**
     *
     * @param uncleanValue
     * @returns {string|number}
     */
    cleanSpecial(uncleanValue) { return this._queryHelper.cleanSpecial(uncleanValue); }

}

module.exports = MysqlDao;
