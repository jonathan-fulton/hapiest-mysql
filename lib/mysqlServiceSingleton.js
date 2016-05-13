'use strict';

const MysqlServiceFactory = require('./MysqlServiceFactory');

let _mysqlService = null;

/**
 * @param {Config} nodeConfig
 */
function initMysqlService(nodeConfig) {
    _mysqlService = MysqlServiceFactory.createFromNodeConfig(nodeConfig);
}

module.exports = {

    /**
     * @param {Config} [nodeConfig]
     * @returns {MysqlService}
     */
    getMysqlService: function(nodeConfig) {
        if (!_mysqlService) {
            initMysqlService(nodeConfig || require('config'));
        }
        return _mysqlService;
    }

};
