'use strict';

const Should = require('should');
const MysqlService = require('../../index');
const MysqlServicePoolConnectionConfigFactory = require('../../lib/mysqlPoolConnectionConfigFactory');

describe('index.js', function() {
   it('Should be a function b/c its a class constructor', function() {
        MysqlService.should.be.a.Function();

       const writeConfig = MysqlServicePoolConnectionConfigFactory.createConnectionConfig({
           host: 'localhost',
           database: 'hapiestmysql',
           user: 'hapiestmysql',
           password: 'hapiestmysql',
           connectionLimit: 1
       });
       const mysqlService = new MysqlService(writeConfig);

       mysqlService.should.be.an.instanceOf(MysqlService);
       mysqlService.selectOne.should.be.a.Function();
       mysqlService.selectOneFromMaster.should.be.a.Function();
       mysqlService.selectAll.should.be.a.Function();
       mysqlService.selectAllFromMaster.should.be.a.Function();
       mysqlService.insert.should.be.a.Function();
       mysqlService.update.should.be.a.Function();
       mysqlService.delete.should.be.a.Function();
       mysqlService.clean.should.be.a.Function();
   });
});