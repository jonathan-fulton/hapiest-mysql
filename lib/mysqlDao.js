'use strict';

const Promise = require('bluebird');
const _ = require('lodash');
const MysqlDaoQueryHelper = require('./mysqlDaoQueryHelper');
const TransformStream = require('stream').Transform;

class MysqlDao {

    /**
     * @name {MysqlDaoGetAllOptions}
     * @type {Object}
     * @property {Object} [sort] - properties should be column names, values are 'asc'/'desc'
     * @property {int} [limit]
     * @property {int} [offset] - you have to provide a limit if you provide an offset
     */

    /**
     * @name {MysqlDaoGetAllAndCountResult}
     * @type {Object}
     * @property {Array[dbRows]} [results]
     * @property {int} [count]
     */

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
     * @param {object} createArgs column values to be inserted
     * @param {object} [opts] additional insert options
     * @param {boolean} [opts.ignoreOnDuplicateKey] if true, insert will ignore duplicate keys
     * @returns {Promise.<int,Error>} - ID of last inserted item; or 0 if ignoreOnDuplicateKey is true and nothing new was inserted
     */
    create(createArgs, opts) {
        const sql = this._queryHelper.create(createArgs, opts);
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
     * @param {object} [opts] additional insert options
     * @param {boolean} [opts.ignoreOnDuplicateKey] if true, insert will ignore duplicate keys
     * @returns {Promise.<int,Error>} - number of processed rows (including ignored duplicates)
     */
    createBulk(createArgsArr, opts) {
        const sql = this._queryHelper.createBulk(createArgsArr, opts);
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
     * @param {object} insertArgs
     * @param {object} onDupUpdateArgs
     * @returns {Promise.<int,Error>} - ID of last inserted item
     */
    upsert(insertArgs, onDupUpdateArgs) {
        const sql = this._queryHelper.upsert(insertArgs, onDupUpdateArgs);
        return this.upsertFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<int,Error>}
     */
    upsertFromSql(sql) {
        return this._mysqlService.upsert(sql)
            .then(results => results.affectedRows )
            .catch(err => {
                this._logger.error(err.message, {sql: sql, err:err});
                throw new Error('MysqlDao.upsertFromSql() failed');
            });
    }

    /**
     * @param {object} insertArgs
     * @param {object|Array} onDupUpdateArgs  If an object, the columns specified by the object keys will be
     * updated with the corresponding values.  (This uses the column = value syntax.)  If an array, the columns
     * listed in the array will be updated with the insert values.  (This uses the column = VALUES(column) syntax.)
     * @returns {Promise.<int|Error>} The number of affected rows
     */
    upsertBulk(insertArgs, onDupUpdateArgs) {
        const sql = this._queryHelper.upsertBulk(insertArgs, onDupUpdateArgs);
        return this.upsertBulkFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<int|Error>} The number of affected rows
     */
    upsertBulkFromSql(sql) {
        return this._mysqlService.upsert(sql)
            .then(results => results.affectedRows )
            .catch(err => {
                this._logger.error(err.message, {sql: sql, err:err});
                throw new Error('MysqlDao.upsertFromSql() failed');
            });
    }

    /**
     * @param id
     * @returns {Promise.<object,Error>}
     */
    getOneById(id) {
        return this._getOneById(id);
    }

    /**
     * @param id
     * @returns {Promise.<object,Error>}
     */
    getOneByIdFromMaster(id) {
        return this._getOneById(id, {forceReadFromMaster: true});
    }

    /**
     * @param id
     * @param {MysqlGetFromSqlOptions} options
     * @private
     */
    _getOneById(id, options) {
        return this._getOne({id:id}, options);
    }

    /**
     * @param {object} whereClause
     * @returns {Promise.<object|null,Error>}
     */
    getOne(whereClause) {
        return this._getOne(whereClause);
    }

    /**
     * @param {object} whereClause
     * @returns {Promise.<object|null,Error>}
     */
    getOneFromMaster(whereClause) {
        return this._getOne(whereClause, {forceReadFromMaster: true});
    }

    /**
     * @param {object} whereClause
     * @param {MysqlGetFromSqlOptions} [options]
     * @returns {Promise.<Object|null, Error>}
     * @private
     */
    _getOne(whereClause, options) {
        const sql = this._queryHelper.getOne(whereClause);
        return this._getOneFromSql(sql, options);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object|null, Error>}
     */
    getOneFromSql(sql) {
        return this._getOneFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object|null, Error>}
     */
    getOneFromSqlRaw(sql) {
        return this._getOneFromSql(sql, {rawResults: true});
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object|null, Error>}
     */
    getOneFromSqlFromMaster(sql) {
        return this._getOneFromSql(sql, {forceReadFromMaster: true});
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object|null, Error>}
     */
    getOneFromSqlFromMasterRaw(sql) {
        return this._getOneFromSql(sql, {rawResults: true, forceReadFromMaster: true});
    }


    /**
     * @name MysqlGetFromSqlOptions
     * @type {Object}
     * @property {boolean} [rawResults=false]
     * @property {boolean} [forceReadFromMaster=false]
     */

    /**
     * @param {string} sql
     * @param {MysqlGetFromSqlOptions} [options]
     * @returns {Promise.<object|null,Error>}
     */
    _getOneFromSql(sql, options) {
        const defaultOptions = {
            rawResults: false,
            forceReadFromMaster: false
        };

        /** @type {MysqlGetFromSqlOptions} */
        const optionsToUse = _.defaults(options, defaultOptions);

        const selectFunction = optionsToUse.forceReadFromMaster ?
            this._mysqlService.selectOneFromMaster.bind(this._mysqlService) :
            this._mysqlService.selectOne.bind(this._mysqlService);

        return selectFunction(sql)
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
                throw new Error('MysqlDao._getOneFromSql() failed');
            });
    }

    /**
     * @param {object} whereClause
     * @param {MysqlDaoGetAllOptions}
     * @returns {Promise.<object[],Error>}
     */
    getAll(whereClause, options) {
        options = options || {};
        return this._getAll(whereClause, options);
    }

    /**
     * @param {object} whereClause
     * @param {MysqlDaoGetAllOptions}
     * @returns {Promise.<MysqlDaoGetAllAndCountResult,Error>}
     */
    getAllAndCount(whereClause, options) {
        options = options || {};
        return this._getAllAndCount(whereClause, options);
    }

    /**
     * @param {object} whereClause
     * @param {MysqlDaoGetAllOptions}
     * @returns {Promise.<object[],Error>}
     */
    getAllFromMaster(whereClause, options) {
        options = options || {};
        return this._getAll(whereClause, options, {forceReadFromMaster: true});
    }

    /**
     * @param {object} whereClause
     * @param {MysqlDaoGetAllOptions}
     * @returns {Promise.<MysqlDaoGetAllAndCountResult,Error>}
     */
    getAllAndCountFromMaster(whereClause, options) {
        options = options || {};
        return this._getAllAndCount(whereClause, options, {forceReadFromMaster: true});
    }

    /**
     * @param whereClause
     * @param {MysqlDaoGetAllOptions}
     * @param {MysqlGetFromSqlOptions} [config]
     * @returns {Promise.<Object[], Error>}
     * @private
     */
    _getAll(whereClause, options, config) {
        if (options && options.offset && !options.limit) throw new Error('Missing limit in getAll options, must include with offset option.');
        const sql = this._queryHelper.getAll(whereClause, options);
        return this._getAllFromSql(sql, config);
    }

    /**
     * @param whereClause
     * @param {MysqlDaoGetAllOptions}
     * @param {MysqlGetFromSqlOptions} [config]
     * @returns {Promise.<MysqlDaoGetAllAndCountResult, Error>}
     * @private
     */
    _getAllAndCount(whereClause, options, config) {
        config = config || {};
        if (options && options.offset && !options.limit) throw new Error('Missing limit in getAll options, must include with offset option.');
        const sql = this._queryHelper.getAll(whereClause, options);
        const countSql = this._queryHelper.getCount(whereClause, options);
        const countConfig = _.clone(config);
        countConfig.rawResults = true;
        let results;
        return this._getAllFromSql(sql, config)
        .tap(_results_ => results = _results_)
        .then(_results_ => this._getOneFromSql(countSql, countConfig))
        .then(_count_ => { return {count: _count_.count, results: results} })
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object[], Error>}
     */
    getAllFromSql(sql) {
        return this._getAllFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object[], Error>}
     */
    getCountFromSql(sql) {
        return this._getCountFromSql(sql);
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object[], Error>}
     */
    getAllFromSqlRaw(sql) {
        return this._getAllFromSql(sql, {rawResults: true});
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object|null, Error>}
     */
    getAllFromSqlFromMaster(sql) {
        return this._getAllFromSql(sql, {forceReadFromMaster: true});
    }

    /**
     * @param {string} sql
     * @returns {Promise.<Object|null, Error>}
     */
    getAllFromSqlFromMasterRaw(sql) {
        return this._getAllFromSql(sql, {rawResults: true, forceReadFromMaster: true});
    }

    /**
     * @param {string} sql
     * @param {object} [options]
     * @param {boolean} [options.rawResults=false]
     * @returns {Promise.<object[],Error>}
     */
    _getAllFromSql(sql, options) {
        const defaultOptions = {
            rawResults: false,
            forceReadFromMaster: false
        };

        /** @type {MysqlGetFromSqlOptions} */
        const optionsToUse = _.defaults(options, defaultOptions);

        const selectAllFunction = optionsToUse.forceReadFromMaster ?
            this._mysqlService.selectAllFromMaster.bind(this._mysqlService) :
            this._mysqlService.selectAll.bind(this._mysqlService);

        return selectAllFunction(sql)
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
                throw new Error('MysqlDao._getAllFromSql() failed');
            });
    }

    /**
     * @param {object} whereClause
     * @param {MysqlDaoGetAllOptions} [options={}]
     * @param {ReadableOptions} [streamOptions={}]
     * @returns {stream.Readable}
     */
    stream(whereClause, options, streamOptions) {
        options = options || {};
        streamOptions = streamOptions || {};
        return this._stream(whereClause, options, {}, streamOptions);
    }

    /**
     * @param {object} whereClause
     * @param {MysqlDaoGetAllOptions} [options={}]
     * @param {ReadableOptions} [streamOptions={}]
     * @returns {stream.Readable}
     */
    streamFromMaster(whereClause, options, streamOptions) {
        options = options || {};
        streamOptions = streamOptions || {};
        return this._stream(whereClause, options, {forceReadFromMaster: true}, streamOptions);
    }

    /**
     * @param {string} sql
     * @param {ReadableOptions} [streamOptions={}]
     * @returns {stream.Readable}
     */
    streamFromSql(sql, streamOptions) {
        streamOptions = streamOptions || {};
        return this._streamFromSql(sql, {}, streamOptions);
    }


    /**
     * @param {string} sql
     * @param {ReadableOptions} [streamOptions={}]
     * @returns {stream.Readable}
     */
    streamFromSqlRaw(sql, streamOptions) {
        streamOptions = streamOptions || {};
        return this._streamFromSql(sql, {rawResults: true}, streamOptions);
    }

    /**
     * @param {string} sql
     * @param {ReadableOptions} [streamOptions={}]
     * @returns {stream.Readable}
     */
    streamFromSqlFromMaster(sql, streamOptions) {
        streamOptions = streamOptions || {};
        return this._streamFromSql(sql, {forceReadFromMaster: true}, streamOptions);
    }

    /**
     * @param {string} sql
     * @param {ReadableOptions} [streamOptions={}]
     * @returns {stream.Readable}
     */
    streamFromSqlFromMasterRaw(sql, streamOptions) {
        streamOptions = streamOptions || {};
        return this._streamFromSql(sql, {rawResults: true, forceReadFromMaster: true}, streamOptions);
    }

    /**
     * @param whereClause
     * @param {MysqlDaoGetAllOptions} options
     * @param {MysqlGetFromSqlOptions} [config]
     * @returns {Promise.<Object[], Error>}
     * @private
     */
    _stream(whereClause, options, config) {
        if (options && options.offset && !options.limit) throw new Error('Missing limit in streamAll options, must include with offset option.');
        const sql = this._queryHelper.getAll(whereClause, options);
        return this._streamFromSql(sql, config);
    }

    /**
     * @param {string} sql
     * @param {object} [options]
     * @param {boolean} [options.rawResults=false]
     * @param {ReadableOptions} [streamOptions]
     * @returns {stream.Readable}
    */
    _streamFromSql(sql, options, streamOptions) {

        /** @type {MysqlGetFromSqlOptions} */
        const optionsToUse = _.defaults(options, {
            rawResults: false,
            forceReadFromMaster: false
        });

        /** @type {ReadableOptions} **/
        const streamOptionsToUse = _.defaults(streamOptions, {
            objectMode: true
        });

        const streamFunction = optionsToUse.forceReadFromMaster ?
        this._mysqlService.streamQueryFromMaster.bind(this._mysqlService) :
        this._mysqlService.streamQuery.bind(this._mysqlService);

        /** @type {stream.Readable} **/
        const queryStream = streamFunction(sql, streamOptionsToUse)
        .on('error', (err) => {
            this._logger.error(err.message, { sql: sql, err:err });
        });

        if (!optionsToUse.rawResults) {
            const transform = new this._TransformResult(streamOptionsToUse);
            queryStream.on('error', (err) => {
                transform.emit('error', err);
                transform.end();
            });
            return queryStream.pipe(transform);
        }

        return queryStream;
    }

    get _TransformResult() {
        const dao = this;
        class TransformResult extends TransformStream {
            _transform(dbRow, encoding, cb) {
                cb(null, dao._createVoFromDbRow(dbRow));
            }
        }
        return TransformResult;
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

    /**
     * Similar to join(), but will divide `objectsToJoin` into batches of size `maxBatchSize`
     * @param {object[]|object} objectsToJoin
     * @param {string} lookupKey
     * @param {object} options
     * @param {int} [options.maxBatchSize = 100] - The max batch size for querying this Dao's table.
     * @param {string} [options.joinKey = "id"]
     * @param {string} [options.resultsKey]
     * @return {Promise.<object[]|object>}
     */
    batchJoin(objectsToJoin, lookupKey, options) {

        if (!objectsToJoin) {
            return Promise.resolve();
        }

        const shouldReturnArray = _.isArray(objectsToJoin);
        objectsToJoin = _.castArray(objectsToJoin);
        options = _.defaults(options, {
            joinKey: 'id',
            resultsKey: _.camelCase(this.tableName),
            maxBatchSize: 100
        });

        // Split into batches based on batch size.
        const objectBatches = _.chunk(objectsToJoin, options.maxBatchSize);

        // Process each batch, then recombine.
        return Promise.map(objectBatches, objectBatch =>
            this.join(objectBatch, lookupKey, options)
        )
            .then(_.flatten)
            .then(objectsWithJoins => {
                return shouldReturnArray ? objectsWithJoins : _.head(objectsWithJoins);
            });

    }

    /**
     * Takes an array of objects and joins entries from this Dao onto each of them. It will take "joinKey" from each object
     * (defaulting to "id") and query this Dao using "lookupKey" for all rows matching that array of values (`SELECT * FROM table_name WHERE id IN (x, y, z, ...)`),
     * and then join the matching VOs and add them under a new property ("resultsKey") on the original object.
     * @param {object[]|object} objectsToJoin - Object or objects which should be joined-onto.
     * @param {string} lookupKey - The lookup key to use for selecting entities from this Dao.
     * @param {object} options
     * @param {string} [options.joinKey = "id"] - The join key to use for the objects in objectsToJoin.
     * @param {string} [options.resultsKey] - The key onto which the joined entities will be added. Defaults to the camelCase value of `this.tableName`.
     * @return {Promise.<object[]|object>} - Returns original input object(s) decorated with joined VOs under "resultsKey"
     */
    join(objectsToJoin, lookupKey, options) {

        if (!objectsToJoin) {
            return Promise.resolve();
        }

        const shouldReturnArray = _.isArray(objectsToJoin);
        objectsToJoin = _.castArray(objectsToJoin);
        options = _.defaults(options, {
            joinKey: 'id',
            resultsKey: _.camelCase(this.tableName)
        });

        const joinValues = objectsToJoin.map(object => object[options.joinKey]);

        return this.getAll({[lookupKey]: joinValues})
            .then(vos => {

                // For each object in the join, find all matching VOs from this Dao to join.
                return Promise.map(objectsToJoin, object => {

                    // Pull all vos which match this object. NOTE: This mutates `vos` (reducing the set each time).
                    const vosToJoin = _.remove(vos, vo => object[options.joinKey] === vo[lookupKey]);

                    // Add the matching vos for the join under `resultsKey`.
                    object[options.resultsKey] = vosToJoin;
                    return object;
                });
            })
            .then(objectsWithJoins => {
                return shouldReturnArray ? objectsWithJoins : _.head(objectsWithJoins);
            })
    }

}

module.exports = MysqlDao;
