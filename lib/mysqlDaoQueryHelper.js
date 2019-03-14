'use strict';

const _ = require('lodash');
const Squel = require('squel');
const VO = require('hapiest-vo');
const Moment = require('moment');

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
            like: 'LIKE',
            lt: '<',
            lte: '<=',
            eq: '=',
            ne: '!=',
            in: 'IN',
            nin: 'NOT IN',
            not_in: 'NOT IN', // supporting a more readable version of nin
            raw: ''
        };

        this._squel = Squel.useFlavour('mysql');
    }

    /**
     * @param {object} createArgs
     * @param {object} [opts] additional insert options
     * @param {boolean} [opts.ignoreOnDuplicateKey] if true, insert will ignore duplicate keys
     * @returns {string}
     */
    create(createArgs, opts) {
        return this.createBulk([createArgs], opts);
    }

    /**
     * @param {Array.<object>} createArgsArray
     * @param {object} [opts] additional insert options
     * @param {boolean} [opts.ignoreOnDuplicateKey=false] if true, insert will ignore duplicate keys
     * @param {boolean} [opts.dontCleanMysqlFunctions=true] allow some mysql expressions in your VALUES.
     *   Disable for a small performance improvenment.
     * @returns {string}
     */
    createBulk(createArgsArray, opts = {}) {
        const cleanValues = this._cleanAndMapValues(createArgsArray, opts.dontCleanMysqlFunctions);

        const sqlBuilder =
            this._squel.insert({autoQuoteFieldNames: true}).into(this._tableName)
                .setFieldsRows(cleanValues, {dontQuote: true});

        if (opts.ignoreOnDuplicateKey) {
            //INSERT ... IGNORE actually ignores ALL errors, not just duplicate key, so best practice is to "update"
            // a duplicate setting a column to itself
            const firstField = Object.keys(cleanValues[0])[0];
            sqlBuilder.onDupUpdate(`\`${firstField}\``, `\`${firstField}\``, {dontQuote: true})
        }

        return sqlBuilder.toString();
    }

    /**
     * @param {object} insertArgs
     * @param {object} onDupUpdateArgs
     * @returns {string}
     */
    upsert(insertArgs, onDupUpdateArgs) {
        const cleanInsertValues = this._cleanAndMapValues(insertArgs);
        const cleanOnDupUpdateValues = this._cleanAndMapValues(onDupUpdateArgs);

        let upsert =
            this._squel.insert({autoQuoteFieldNames: true}).into(this._tableName)
                .setFields(cleanInsertValues, {dontQuote: true});

        Object.keys(cleanOnDupUpdateValues).forEach(property => {
            const cleanUpsertValue = cleanOnDupUpdateValues[property];
            upsert.onDupUpdate(`\`${property}\``, cleanUpsertValue, {dontQuote: true});
        });
        return upsert.toString();
    }

    /**
     * @param {Array.<object>} insertArgsArray
     * @param {object|Array} onDupUpdateArgs If an object, update columns with values defined by the object keys.  If
     *    an array, update the listed columns with the insert value.
     * @param {object} [opts] additional insert options
     * @param {boolean} [opts.dontCleanMysqlFunctions=true] allow some mysql expressions in your VALUES
     *    Disable for a small performance improvenment.
     * @returns {string}
     */
    upsertBulk(insertArgsArray, onDupUpdateArgs, opts = {}) {

        const cleanValues = this._cleanAndMapValues(insertArgsArray, opts.dontCleanMysqlFunctions);

        const upsert =
            this._squel.insert({autoQuoteFieldNames: true}).into(this._tableName)
                .setFieldsRows(cleanValues, {dontQuote: true});

        if (_.isArray(onDupUpdateArgs)) {
            onDupUpdateArgs.forEach(propName => {
                const snakeCaseProp = _.snakeCase(propName);
                upsert.onDupUpdate(`\`${snakeCaseProp}\``, "VALUES(`" + snakeCaseProp + "`)", {dontQuote: true});
            });
        } else {
            const cleanOnDupUpdateValues = this._cleanAndMapValues(onDupUpdateArgs);
            Object.keys(cleanOnDupUpdateValues).forEach(property => {
                const cleanUpsertValue = cleanOnDupUpdateValues[property];
                upsert.onDupUpdate(`\`${property}\``, cleanUpsertValue, {dontQuote: true});
            });
        }


        return upsert.toString();
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
        if (['current_timestamp','now()','is null', 'is not null'].indexOf(_.toLower(uncleanValue)) !== -1) {
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
            this._squel.select({parameterCharacter: '!!@@##$$%%', autoQuoteFieldNames: true})
                .from(this._tableName);

        sqlObject = this._appendWhereClause(sqlObject, whereClause);

        if (options.sort) {
            Object.keys(options.sort).forEach(key => {
                sqlObject = sqlObject.order(`\`${_.snakeCase(String(key))}\``, String(options.sort[key]).toUpperCase() !== 'DESC');
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
            this._squel.select({parameterCharacter: '!!@@##$$%%'})
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
            this._squel.update({parameterCharacter: '!!@@##$$%%', autoQuoteFieldNames: true}) // See comment in _getBase for explanation of paramChar - using sequence likely never to be inserted
                .table(this._tableName)
                .setFields(cleanUpdateValues, {dontQuote: true});

        sqlObject = this._appendWhereClause(sqlObject, whereClause);
        return sqlObject;
    }

    _deleteBase(whereClause) {
        let sqlObject =
            this._squel.delete({parameterCharacter: '!!@@##$$%%', autoQuoteFieldNames: true}) // See comment in _getBase for explanation of paramChar
                .from(this._tableName);

        sqlObject = this._appendWhereClause(sqlObject, whereClause);
        return sqlObject;
    }

    /**
     * @private
     * @param {Object|Object[]} obj
     * @param {Boolean} [dontCleanMysqlFunctions] whether or not to allow MySQL functions to exist in values
     * @returns {Object} - cleans the values and returns an object with snake_case props --> clean values
     */
    _cleanAndMapValues(obj, dontCleanMysqlFunctions = true) {
        if (Array.isArray(obj)) {
            const propertyMappings = MysqlDaoQueryHelper._getPropertyMappings(obj[0]);
            return obj.map(obj => this._cleanAndMapObjectProperties(obj, propertyMappings, dontCleanMysqlFunctions));
        } else {
            const propertyMappings = MysqlDaoQueryHelper._getPropertyMappings(obj);
            return this._cleanAndMapObjectProperties(obj, propertyMappings, dontCleanMysqlFunctions);
        }
    }

    /**
     * @private
     * @param {Object} obj
     * @returns {Object} - a lookup object mapping property names to their snake_cased equivalents
     */
    static _getPropertyMappings(obj) {
        if (typeof(obj.toJsObj) === 'function') {
            obj = obj.toJsObj();
        } else if (typeof(obj.toJSON) === 'function') {
            obj = obj.toJSON();
        }

        const mappings = {};
        Object.keys(obj).forEach(key => {
            mappings[key] = key.includes('->') ? key : _.snakeCase(key);
        });
        return mappings;
    }

    /**
     * Cleans values and maps them to snake_cased property names.
     * This method should only be called by _cleanAndMapValues()
     * @private
     * @param {object} obj
     * @param {Object} propertyMappings maps property names to their snake_cased equivalents
     * @param {Boolean} dontCleanMysqlFunctions whether or not to allow MySQL functions to exist in values
     * @returns {object} - cleans the values and returns an object with snake_case props --> clean values
     */
    _cleanAndMapObjectProperties(obj, propertyMappings, dontCleanMysqlFunctions) {
        if (typeof(obj.toJsObj) === 'function') {
            obj = obj.toJsObj();
        } else if (typeof(obj.toJSON) === 'function') {
            obj = obj.toJSON();
        }

        const cleanValues = {};
        const clean = dontCleanMysqlFunctions ? this.cleanSpecial.bind(this) : this.clean.bind(this);

        Object.keys(obj).forEach(property => {
            const uncleanValue = obj[property];
            const mappedProp = propertyMappings[property];
            if (uncleanValue === null || typeof uncleanValue === 'string' || typeof uncleanValue === 'number' || typeof uncleanValue === 'boolean' || typeof uncleanValue === 'undefined') {
                // Escaping primitive values
                cleanValues[mappedProp] = clean(uncleanValue);
            } else if (_.has(uncleanValue, 'raw')) {
                cleanValues[mappedProp] = uncleanValue.raw;
            } else if (_.isDate(uncleanValue) || uncleanValue instanceof Moment) {
                cleanValues[mappedProp] = clean(uncleanValue);
            } else if (_.isArray(uncleanValue)) {
                // Escaping array values
                const cleanValueArray = [];
                uncleanValue.forEach(uncleanVal => {
                    if (['string', 'number', 'boolean'].indexOf(typeof(uncleanVal)) !== -1) {
                        cleanValueArray.push(clean(uncleanVal))
                    }
                });
                if (cleanValueArray.length > 0) {
                    cleanValues[mappedProp] = cleanValueArray;
                }
            } else if (_.isObject(uncleanValue)) {
                this._validateQueryObject(uncleanValue);
                // Escape the values in the operator object
                const cleanQueryObj = this._cleanAndMapValues(uncleanValue, dontCleanMysqlFunctions);
                if (_.size(cleanQueryObj) > 0) {
                    cleanValues[mappedProp] = cleanQueryObj;
                }
            }
        });

        return cleanValues;
    }

    _appendWhereClause(sqlObject, whereClause) {
        const cleanValues = this._cleanAndMapValues(whereClause);
        Object.keys(cleanValues).forEach(columnName => {
            const whereClauseKey = columnName.includes('->') ? columnName : _.camelCase(columnName);
            const uncleanValue = whereClause[whereClauseKey];
            const cleanValue = cleanValues[columnName];

            if (uncleanValue === null || typeof(uncleanValue) === 'undefined') {
                sqlObject = sqlObject.where(`\`${columnName}\` IS NULL`);
            } else if (cleanValue === 'IS NULL' || cleanValue === 'IS NOT NULL') {
                sqlObject = sqlObject.where(`\`${columnName}\` ${cleanValue}`);
            } else if (_.isArray(cleanValue)) {
                const cleanValueList = cleanValue.join(',');
                sqlObject = sqlObject.where(`\`${columnName}\` IN (${cleanValueList})`)
            } else if (_.isObject(cleanValue)) {
                sqlObject = sqlObject.where(this._queryObjectToSql(columnName, cleanValue));
            } else if (columnName.includes('->')) {
                let jsonPathWithQuotedColumn = columnName
                    .split('->')
                    .map((path, index) => {
                        const isFieldName = index === 0;
                        const isQuoted = path[0] === '`' && path[path.length - 1] === '`';

                        return isFieldName && !isQuoted ? `\`${path}\`` : path;
                    })
                    .join('->');
                sqlObject = sqlObject.where(`${jsonPathWithQuotedColumn} = ${cleanValue}`);
            } else {
                sqlObject = sqlObject.where(`\`${columnName}\` = ${cleanValue}`);
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
            const cleanQueryOperator = this._cleanQueryOperator(operator, value);
            const cleanedValue = this._cleanValueForOperator(operator, value);
            sql += `\`${columnName}\` ${cleanQueryOperator} ${cleanedValue}`;
        });

        return sql;
    }

    /**
     * Performs any additional cleaning necessary for the specified operation.  This function assumes that the value
     * has already been cleaned and transformed into a safe SQL value.
     * @param operator
     * @param value
     * @returns {*}
     * @private
     */
    _cleanValueForOperator(operator, value) {
        if (_.includes(['in', 'nin', 'not_in'], operator)) {
            value = _.castArray(value);
            const joinedValues = value.join(',');
            return `(${joinedValues})`;
        }

        return value;
    }

    _cleanQueryOperator(operator, value) {
        let cleanOperator = this._queryOperators[operator];
        if (operator === 'ne' && value === 'NULL') {
            cleanOperator = 'IS NOT';
        }
        return cleanOperator;
    }

}

module.exports = MysqlDaoQueryHelper;
