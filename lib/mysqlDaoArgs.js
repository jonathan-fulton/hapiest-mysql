'use strict';

const VO = require('hapiest-vo');

class MysqlDaoArgs extends VO {

   /**
    * @param {object} obj
    * @param {MysqlService} obj.mysqlService
    * @param {string} obj.tableName
    * @param {function} obj.createVoFromDbRowFunction
    * @param {Logger} obj.logger
    */
    constructor(obj) {
        super();
        this._addProperties(obj);
    }

   /**
    * @returns {MysqlService}
    */
    get mysqlService() {
        return this.get('mysqlService');
    }

   /**
    * @returns {string}
    */
    get tableName() {
        return this.get('tableName');
    }

   /**
    * @returns {function}
    */
    get createVoFromDbRowFunction() {
        return this.get('createVoFromDbRowFunction');
    }

   /**
    * @returns {Logger}
    */
    get logger() {
        return this.get('logger');
    }

}

module.exports = MysqlDaoArgs;