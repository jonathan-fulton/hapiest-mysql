'use strict';

const Should = require('should');
const Index = require('../../index');
const MysqlServicePoolConnectionConfigFactory = require('../../lib/mysqlPoolConnectionConfigFactory');

describe('index.js', function() {
   it('Should expose MysqlServiceFactory and MysqlDaoArgsFactory', function() {
       Index.should.have.properties('serviceFactory','daoArgsFactory');

       Index.serviceFactory.should.have.properties('createFromNodeConfig', 'createFromObjWithOnePool', 'createFromObj', 'create');
       Index.serviceFactory.createFromNodeConfig.should.be.a.Function();
       Index.serviceFactory.createFromObjWithOnePool.should.be.a.Function();
       Index.serviceFactory.createFromObj.should.be.a.Function();
       Index.serviceFactory.create.should.be.a.Function();

       Index.daoArgsFactory.should.have.properties('createFromJsObj');
       Index.daoArgsFactory.createFromJsObj.should.be.a.Function();
   });
});