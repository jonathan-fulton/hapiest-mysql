'use strict';

const _ = require('lodash');
const Squel = require('squel');
const VO = require('hapiest-vo');

class MysqlDaoQueryHelper {

    constructor(tableName, cleanFunction) {
        this._tableName = tableName;
        this._clean = cleanFunction;
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

    /**
     * @param {object} whereClause - key/value pairs that are "and"ed together
     */
    getOne(whereClause) {
        const sqlObject = this._getBase(whereClause);
        const sql = sqlObject.limit(1).toString();
        return sql;
    }

    getAll(whereClause) {
        const sqlObject = this._getBase(whereClause);
        const sql = sqlObject.toString();
        return sql;
    }

    updateOne(whereClause, updateArgs) {
        const sqlObject = this._updateBase(whereClause, updateArgs);
        const sql = sqlObject.limit(1).toString();
        return sql;
    }
    
    deleteOne(whereClause) {
        const sqlObject = this._deleteBase(whereClause);
        const sql = sqlObject.limit(1).toString();
        return sql;
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
     * @returns {Squel}
     * @private
     */
    _getBase(whereClause) {
        // Note, this is a really hacky way of telling Squel to ignore all ? characters
        // Take advantage that it uses str.substr(idx, paramChar.length) === paramChar to determine whether or not to
        // replace characters.  Before this, a URL with query parameter such as youtube.com/?q=abc would cause the
        // ? to be replaced with 'undefined'.  Stupidly annoying!
        let sqlObject =
            Squel.select({parameterCharacter: []})
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
            Squel.delete({parameterCharacter: []}) // See comment in _getBase for explanation of paramChar
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

            // Escaping primitive values
            if (_.includes(['string', 'number', 'boolean', 'undefined'], typeof(uncleanValue)) || uncleanValue === null) {
                const cleanValue = config.dontCleanMysqlFunctions ? this.cleanSpecial(uncleanValue) : this.clean(uncleanValue) ;
                cleanValues[snakeCaseProperty] = cleanValue;
            }
            // Escaping array values
            else if (Array.isArray(uncleanValue)) {
                const cleanValueArray = [];
                uncleanValue.forEach(uncleanVal => {
                    if (_.includes(['string', 'number', 'boolean'], typeof(uncleanVal))) {
                        const cleanVal = config.dontCleanMysqlFunctions ? this.cleanSpecial(uncleanVal) : this.clean(uncleanVal) ;
                        cleanValueArray.push(cleanVal)
                    }
                });
                if (cleanValueArray.length > 0) {
                    cleanValues[snakeCaseProperty] = cleanValueArray;
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
            } else if (Array.isArray(cleanValue)) {
                const cleanValueList = cleanValue.join(',');
                sqlObject = sqlObject.where(`${columnName} IN (${cleanValueList})`)
            } else {
                sqlObject = sqlObject.where(`${columnName} = ${cleanValue}`);
            }
        });
        return sqlObject;
    }
}

module.exports = MysqlDaoQueryHelper;