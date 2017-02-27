'use strict';

class MysqlQuery {
    static validateSelect(query) {
        if (!MysqlQuery.isSelect(query)) {
            throw new Error("Invalid SELECT query");
        }
    }

    static validateInsert(query) {
        if (!MysqlQuery.isInsert(query)) {
            throw new Error("Invalid INSERT query");
        }
    }

    static validateUpdate(query) {
        if (!MysqlQuery.isUpdate(query)) {
            throw new Error("Invalid UPDATE query");
        }
    }

    static validateUpsert(query) {
        if (!MysqlQuery.isUpsert(query)) {
            throw new Error("Invalid INSERT ... ON DUPLICATE KEY UPDATE query");
        }
    }

    static validateDelete(query) {
        if (!MysqlQuery.isDelete(query)) {
            throw new Error("Invalid DELETE query");
        }
    }

    static isSelect(query) {return MysqlQuery._isType(query, 'select');}
    static isInsert(query) {return MysqlQuery._isType(query, 'insert');}
    static isUpdate(query) {return MysqlQuery._isType(query, 'update');}
    static isUpsert(query) {return MysqlQuery._isType(query, 'insert') && MysqlQuery._contains(query, 'on duplicate key update');}
    static isDelete(query) {return MysqlQuery._isType(query, 'delete');}

    static _isType(query, type) {
        const regexString = `^${type}`;
        const regex = new RegExp(regexString, 'i');
        const convertedQuery = query.trim().toLowerCase();
        return regex.test(convertedQuery);
    }

    static _contains(query, extra) {
        const regexString = `${extra}`;
        const regex = new RegExp(regexString, 'i');
        const convertedQuery = query.trim().toLowerCase();
        return regex.test(convertedQuery);
    }

}

module.exports = MysqlQuery;