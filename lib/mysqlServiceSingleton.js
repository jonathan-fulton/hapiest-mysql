'use strict';

const MysqlServiceFactory = require('./MysqlServiceFactory');

let _mysqlService = null;

function initMysqlService() {
    _mysqlService = MysqlServiceFactory.createFromNodeConfig();
}

module.exports = {

    /**
     * @returns {MysqlService}
     */
    getMysqlService: function() {
        if (!_mysqlService) {
            initMysqlService();
        }
        return _mysqlService;
    }

};
