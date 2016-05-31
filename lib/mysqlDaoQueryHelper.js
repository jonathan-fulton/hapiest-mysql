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
            Squel.update()
                .table(this._tableName)
                .setFields(cleanUpdateValues, {dontQuote: true});

        sqlObject = this._appendWhereClause(sqlObject, whereClause);
        return sqlObject;
    }
    
    _deleteBase(whereClause) {
        let sqlObject =
            Squel.delete()
                .from(this._tableName);

        sqlObject = this._appendWhereClause(sqlObject, whereClause);
        return sqlObject;
    }

    /**
     * @param {object} obj
     * @returns {object} - cleans the values and returns an object with snake case props --> clean values
     */
    _cleanAndMapValues(obj) {
        if (obj instanceof VO) {
            obj = obj.toJsObj();
        }

        const properties = Object.keys(obj);
        const cleanValues = {};

        properties.forEach(property => {
            const uncleanValue = obj[property];

            // Automatically drop Array, Objects, undefined which makes saves times mapping stuff when you have a complicated object structure group multiple tables
            if (_.includes(['string', 'number', 'boolean'], typeof(uncleanValue)) || uncleanValue === null) {
                const cleanValue = this._clean(uncleanValue);
                const snakeCaseProperty = _.snakeCase(property);
                cleanValues[snakeCaseProperty] = cleanValue;
            }
        });

        return cleanValues;
    }

    _appendWhereClause(sqlObject, whereClause) {
        const cleanValues = this._cleanAndMapValues(whereClause);
        Object.keys(cleanValues).forEach(columnName => {
            const cleanValue = cleanValues[columnName];
            sqlObject = sqlObject.where(`${columnName} = ${cleanValue}`);
        });
        return sqlObject;
    }
}

module.exports = MysqlDaoQueryHelper;