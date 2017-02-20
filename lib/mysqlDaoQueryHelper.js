'use strict';

const _ = require('lodash');
const Squel = require('squel');
const VO = require('hapiest-vo');

class MysqlDaoQueryHelper {

    /**
     * @name {MysqlDaoGetAllOptions}
     * @type {Object}
     * @property {Object} [sort] - properties should be column names, values are 'asc'/'desc'
     * @property {int} [limit]
     * @property {int} [offset] - you have to provide a limit if you provide an offset
     */

    constructor(tableName, cleanFunction) {
        this._tableName = tableName;
        this._clean = cleanFunction;
        this._queryOperators = {
            gt: '>',
            gte: '>=',
            lt: '<',
            lte: '<=',
            eq: '=',
            ne: '!='
        };
    }

    /**
     * @param {object} createArgs
     * @returns {string}
     */
    create(createArgs) {
        const cleanValues = this._cleanAndMapValues(createArgs);

        const sql =
            Squel.insert().into(this._tableName)
                .setFields(cleanValues, {dontQuote: true})
                .toString();
        return sql;
    }

    /**
     * @param {Array.<object>} createArgsArray
     */
    createBulk(createArgsArray) {
        const cleanValues = [];
        createArgsArray.forEach(createArgs => {
            cleanValues.push(this._cleanAndMapValues(createArgs));
        });

        const sql =
            Squel.insert().into(this._tableName)
                .setFieldsRows(cleanValues, {dontQuote: true})
                .toString();
        return sql;
    }

    upsert(upsertArgs) {
        const cleanValues = this._cleanAndMapValues(upsertArgs);
        let insert =
            Squel.useFlavour('mysql').insert().into(this._tableName)
                .setFields(cleanValues, {dontQuote: true});

        Object.keys(cleanValues).forEach(property => {
            insert.onDupUpdate(property, cleanValues[property], {dontQuote: true});
        });

        return insert.toString();
    }

    /**
     * @param {object} whereClause - key/value pairs that are "and"ed together
     */
    getOne(whereClause) {
        const sqlObject = this._getBase(whereClause);
        const sql = sqlObject.limit(1).toString();
        return sql;
    }

    getAll(whereClause, options) {
        const sqlObject = this._getBase(whereClause, options);
        const sql = sqlObject.toString();
        return sql;
    }

    getCount(whereClause, options) {
        const sqlObject = this._getCountBase(whereClause, options);
        const sql = sqlObject.toString();
        return sql;
    }

    updateOne(whereClause, updateArgs) {
        const sqlObject = this._updateBase(whereClause, updateArgs);
        const sql = sqlObject.limit(1).toString();
        return sql;
    }

    update(whereClause, updateArgs) {
        const sqlObject = this._updateBase(whereClause, updateArgs);
        return sqlObject.toString();
    }

    deleteOne(whereClause) {
        const sqlObject = this._deleteBase(whereClause);
        const sql = sqlObject.limit(1).toString();
        return sql;
    }

    delete(whereClause) {
        const sqlObject = this._deleteBase(whereClause);
        return sqlObject.toString();
    }

    /**
     * @returns {Squel}
     */
    squel() {
        return Squel;
    }

    /**
     * Raw MySQL clean function
     * @param uncleanValue
     * @returns {string|number}
     */
    clean(uncleanValue) {
        return this._clean(uncleanValue);
    }

    /**
     * Allows special MySQL functions to pass through without getting cleaned
     * @param uncleanValue
     * @returns {string|number}
     */
    cleanSpecial(uncleanValue) {
        let cleanValue = null;
        if (_.includes(['current_timestamp','now()','is null', 'is not null'],_.toLower(uncleanValue))) {
            cleanValue = uncleanValue;
        } else {
            const valueToClean = typeof(uncleanValue) === 'undefined' ? null : uncleanValue;
            cleanValue = this._clean(valueToClean);
        }
        return cleanValue;
    }

    /**
     * @param {object} whereClause
     * @param {MysqlDaoGetAllOptions}
     * @returns {Squel}
     * @private
     */
    _getBase(whereClause, options) {
        options = options || {};
        // Note, this is a really hacky way of telling Squel to ignore all ? characters
        // Take advantage that it uses str.substr(idx, paramChar.length) === paramChar to determine whether or not to
        // replace characters.  Before this, a URL with query parameter such as youtube.com/?q=abc would cause the
        // ? to be replaced with 'undefined'.  Stupidly annoying!
        let sqlObject =
            Squel.select({parameterCharacter: '!!@@##$$%%'})
                .from(this._tableName);

        sqlObject = this._appendWhereClause(sqlObject, whereClause);

        if (options.sort) {
            Object.keys(options.sort).forEach(key => {
                sqlObject = sqlObject.order(_.snakeCase(String(key)), String(options.sort[key]).toUpperCase() !== 'DESC');
            })
        }
        if (options.limit) sqlObject = sqlObject.limit(Number(options.limit));
        if (options.offset) sqlObject = sqlObject.offset(Number(options.offset));

        return sqlObject;
    }

    /**
     * @param {object} whereClause
     * @param {MysqlDaoGetAllOptions}
     * @returns {Squel}
     * @private
     */
    _getCountBase(whereClause, options) {
        options = options || {};
        let sqlObject =
            Squel.select({parameterCharacter: '!!@@##$$%%'})
            .field('COUNT(*)', "count")
            .from(this._tableName);

        sqlObject = this._appendWhereClause(sqlObject, whereClause);

        return sqlObject;
    }

    /**
     * @param whereClause
     * @param updateArgs
     * @returns {Squel}
     * @private
     */
    _updateBase(whereClause, updateArgs) {
        const cleanUpdateValues = this._cleanAndMapValues(updateArgs);

        let sqlObject =
            Squel.update({parameterCharacter: '!!@@##$$%%'}) // See comment in _getBase for explanation of paramChar - using sequence likely never to be inserted
                .table(this._tableName)
                .setFields(cleanUpdateValues, {dontQuote: true});

        sqlObject = this._appendWhereClause(sqlObject, whereClause);
        return sqlObject;
    }

    _deleteBase(whereClause) {
        let sqlObject =
            Squel.delete({parameterCharacter: '!!@@##$$%%'}) // See comment in _getBase for explanation of paramChar
                .from(this._tableName);

        sqlObject = this._appendWhereClause(sqlObject, whereClause);
        return sqlObject;
    }

    /**
     * @param {object} obj
     * @param {object} [config]
     * @param {boolean} [config.dontCleanMysqlFunctions] - defaults to true; includes CURRENT_TIMESTAMP and NOW();
     *
     * @returns {object} - cleans the values and returns an object with snake case props --> clean values
     */
    _cleanAndMapValues(obj, config) {
        const defaultConfig = {
            dontCleanMysqlFunctions: true
        };

        config = _.merge(defaultConfig, config);

        if (obj instanceof VO) {
            obj = obj.toJsObj();
        } else if (typeof(obj.toJsObj) === 'function') {
            obj = obj.toJsObj();
        } else if (typeof(obj.toJSON) === 'function') {
            obj = obj.toJSON();
        }

        const properties = Object.keys(obj);
        const cleanValues = {};

        properties.forEach(property => {
            const uncleanValue = obj[property];
            const snakeCaseProperty = _.snakeCase(property);

            if (_.includes(['string', 'number', 'boolean', 'undefined'], typeof(uncleanValue)) || uncleanValue === null) {
                // Escaping primitive values
                const cleanValue = config.dontCleanMysqlFunctions ? this.cleanSpecial(uncleanValue) : this.clean(uncleanValue);
                cleanValues[snakeCaseProperty] = cleanValue;
            } else if (_.isDate(uncleanValue)) {
                //TODO: Handle timezones using a config setting?
                const cleanValue = config.dontCleanMysqlFunctions ? this.cleanSpecial(uncleanValue) : this.clean(uncleanValue);
                cleanValues[snakeCaseProperty] = cleanValue;
            } else if (_.isArray(uncleanValue)) {
                // Escaping array values
                const cleanValueArray = [];
                uncleanValue.forEach(uncleanVal => {
                    if (_.includes(['string', 'number', 'boolean'], typeof(uncleanVal))) {
                        const cleanVal = config.dontCleanMysqlFunctions ? this.cleanSpecial(uncleanVal) : this.clean(uncleanVal);
                        cleanValueArray.push(cleanVal)
                    }
                });
                if (cleanValueArray.length > 0) {
                    cleanValues[snakeCaseProperty] = cleanValueArray;
                }
            } else if (_.isObject(uncleanValue)) {
                this._validateQueryObject(uncleanValue);
                const cleanQueryObj = this._cleanAndMapValues(uncleanValue, config);
                // Escape the values in the operator object
                if (_.size(cleanQueryObj) > 0) {
                    cleanValues[snakeCaseProperty] = cleanQueryObj;
                }
            }

        });

        return cleanValues;
    }

    _appendWhereClause(sqlObject, whereClause) {
        const cleanValues = this._cleanAndMapValues(whereClause);
        Object.keys(cleanValues).forEach(columnName => {
            const whereClauseKey = _.camelCase(columnName);
            const uncleanValue = whereClause[whereClauseKey];
            const cleanValue = cleanValues[columnName];

            if (uncleanValue === null || typeof(uncleanValue) === 'undefined') {
                sqlObject = sqlObject.where(`${columnName} IS NULL`);
            } else if (cleanValue === 'IS NULL' || cleanValue === 'IS NOT NULL') {
                sqlObject = sqlObject.where(`${columnName} ${cleanValue}`);
            } else if (_.isArray(cleanValue)) {
                const cleanValueList = cleanValue.join(',');
                sqlObject = sqlObject.where(`${columnName} IN (${cleanValueList})`)
            } else if (_.isObject(cleanValue)) {
                sqlObject = sqlObject.where(this._queryObjectToSql(columnName, cleanValue));
            } else {
                sqlObject = sqlObject.where(`${columnName} = ${cleanValue}`);
            }
        });
        return sqlObject;
    }

    _validateQueryObject(queryObj) {
        // If the unclean value is a query object, it should be flat and be have only accepted operation keys (gt, lt, eq, ne)
        const possibleKeys = _.keys(this._queryOperators);
        const hasValidKeys = _.difference(_.keys(queryObj), possibleKeys).length === 0;
        // const hasNestedQueryObjects = !!_.find(queryObj, (val, key) => _.isObject(val));
        if (!hasValidKeys) {
            throw new Error('Query objects must be comprised of the accepted operator keys: (' + possibleKeys.join(',') + ').')
        }
    }

    //TODO: Handle OR case.
    _queryObjectToSql(columnName, queryObj) {

        let sql = '';
        let i = 0;
        _.each(queryObj, (value, operator) => {
            if (i++ > 0) {
                sql += ' AND ';
            }
            sql += `${columnName} ${this._queryOperators[operator]} ${value}`;
        });

        return sql;
    }
}

module.exports = MysqlDaoQueryHelper;
