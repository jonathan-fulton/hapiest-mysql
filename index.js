'use strict';

/**
 *
 * @type {{serviceFactory: MysqlServiceFactory, daoArgsFactory: MysqlDaoArgsFactory}}
 */
module.exports = {
    serviceFactory: require('./lib/mysqlServiceFactory'),
    daoArgsFactory: require('./lib/mysqlDaoArgsFactory')
};