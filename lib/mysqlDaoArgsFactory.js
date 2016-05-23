'use strict';

const MysqlDaoArgs = require('./mysqlDaoArgs');

class MysqlDaoArgsFactory {

   /**
    * @param {object} obj
    * @param {MysqlService} obj.mysqlService
    * @param {string} obj.tableName
    * @param {function} obj.createVoFromDbRowFunction
    * @param {Logger} obj.logger
    *
    * @returns {MysqlDaoArgs}
    */
    static createFromJsObj(obj) {
        const newArgs = {
            mysqlService: obj.mysqlService,
            tableName: obj.tableName,
            createVoFromDbRowFunction: obj.createVoFromDbRowFunction,
            logger: obj.logger
        };

       return new MysqlDaoArgs(newArgs);
    }

}

module.exports = MysqlDaoArgsFactory;