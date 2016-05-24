'use strict';

const Should = require('should');
const Mysql = require('mysql');
const VO = require('hapiest-vo');

const MysqlDaoQueryHelper = require('../../lib/mysqlDaoQueryHelper');

const cleanFunction = Mysql.escape;
const mysqlDaoQueryHelper = new MysqlDaoQueryHelper('users', cleanFunction);

class UserCreateArgs extends VO {
    constructor(args) {
        super();
        this._addProperties(args);
    }

    get firstName() { return this.get('firstName'); }
    get lastName() { return this.get('lastName'); }
    get password() { return this.get('password'); }
}

describe('MysqlDaoQueryHelper', function() {

    describe('create', function() {

        it('Should generate an INSERT statement for a single entry in users table', function() {
            const sql = mysqlDaoQueryHelper.create({firstName: 'firstName', lastName: 'lastName', password: 'boom!'});
            Should.exist(sql);

            sql.should.eql(`INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!')`);
        });

        it('Should generate an INSERT statement for a single entry in users table from a VO', function() {
            const sql = mysqlDaoQueryHelper.create(new UserCreateArgs({firstName: 'firstName', lastName: 'lastName', password: 'boom!'}));
            Should.exist(sql);

            sql.should.eql(`INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!')`);
        });

    });

    describe('createBulk', function() {

        it('Should generate an INSERT statement for multiple rows in users table', function() {
            const sql = mysqlDaoQueryHelper.createBulk([
                {firstName: 'firstName', lastName: 'lastName', password: 'boom!'},
                {firstName: 'John', lastName: 'Doe', password: 'badpassword'},
                {firstName: 'Jane', lastName: 'Doe', password: 'foundrydc'}
            ]);
            Should.exist(sql);

            sql.should.eql("INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!'), ('John', 'Doe', 'badpassword'), ('Jane', 'Doe', 'foundrydc')");
        });

    });

    describe('getOne', function() {

        it('Should generate a SELECT statement for a single row', function() {
            const sql = mysqlDaoQueryHelper.getOne({firstName: 'John', lastName: 'Doe'});
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (first_name = 'John') AND (last_name = 'Doe') LIMIT 1");
        });

    });

    describe('getAll', function() {

        it('Should generate a SELECT statement for multiple rows', function() {
            const sql = mysqlDaoQueryHelper.getAll({lastName: 'Doe'});
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (last_name = 'Doe')");
        });

    });

    describe('updateOne', function() {

        it('Should generate an UPDATE statement for one row', function() {
            const sql = mysqlDaoQueryHelper.updateOne({id: 1}, {email: 'john.doe@gmail.com', firstName: 'john'});
            Should.exist(sql);

            sql.should.eql("UPDATE users SET email = 'john.doe@gmail.com', first_name = 'john' WHERE (id = 1) LIMIT 1");
        });

    });

    describe('deleteOne', function() {

        it('Should generate an DELETE statement for one row', function() {
            const sql = mysqlDaoQueryHelper.deleteOne({id: 1});
            Should.exist(sql);

            sql.should.eql("DELETE FROM users WHERE (id = 1) LIMIT 1");
        });

    });

});
