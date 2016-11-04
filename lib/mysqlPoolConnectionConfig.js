'use strict';

const Joi = require('joi');
const VO = require('hapiest-vo');
const MysqlPoolConnectionConfigSchema = require('./mysqlPoolConnectionConfigSchema');

class MysqlPoolConnectionConfig extends VO {

    /**
     * @param {object} config
     */
    constructor(config) {
        super();

        // Validate the config being passed in
        const result = Joi.validate(config, MysqlPoolConnectionConfigSchema);
        if (result.error) { throw result.error; }

        this._addProperties(config);
    }

    /**
     * @returns {string|string[]}
     */
    get host() { return this.get('host'); }
    get port() { return this.get('port'); }
    get user() { return this.get('user'); }
    get password() { return this.get('password'); }
    get database() { return this.get('database'); }
    get connectionLimit() { return this.get('connectionLimit'); }
    get multipleStatements() { return this.get('multipleStatements'); }
    get timezone() { return this.get('timezone') }

}

module.exports = MysqlPoolConnectionConfig;