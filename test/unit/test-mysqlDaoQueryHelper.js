'use strict';

const Should = require('should');
const Mysql = require('mysql');
const Squel = require('squel');
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

        it('Should generate an INSERT statement for a single entry in users table from an object with toJsObj() function defined', function() {
            const obj = {
                toJsObj: function() {
                    return {firstName: 'firstName', lastName: 'lastName', password: 'boom!'};
                }
            };
            const sql = mysqlDaoQueryHelper.create(obj);
            Should.exist(sql);

            sql.should.eql(`INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!')`);
        });

        it('Should generate an INSERT statement for a single entry in users table from an object with toJSON() function defined', function() {
            const obj = {
                toJSON: function() {
                    return {firstName: 'Bob', lastName: 'Smith', password: 'another password'};
                }
            };
            const sql = mysqlDaoQueryHelper.create(obj);
            Should.exist(sql);

            sql.should.eql(`INSERT INTO users (first_name, last_name, password) VALUES ('Bob', 'Smith', 'another password')`);
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

        it('Should allow ? characters in the WHERE clause', function() {
            const sql = mysqlDaoQueryHelper.getOne({url: 'http://www.youtube.com/?q=somevideo'});
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (url = 'http://www.youtube.com/?q=somevideo') LIMIT 1");
        });

        it('Should generate an IS NULL in where clause ', function() {
            const sql = mysqlDaoQueryHelper.getOne({firstName: null, lastName: 'Doe'});
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (first_name IS NULL) AND (last_name = 'Doe') LIMIT 1");
        });

    });

    describe('getAll', function() {

        it('Should generate a SELECT statement for multiple rows', function() {
            const sql = mysqlDaoQueryHelper.getAll({lastName: 'Doe'});
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (last_name = 'Doe')");
        });

        it('Should allow ? characters in the WHERE clause', function() {
            const sql = mysqlDaoQueryHelper.getAll({url: 'http://www.youtube.com/?q=somevideo'});
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (url = 'http://www.youtube.com/?q=somevideo')");
        });

    });

    describe('updateOne', function() {

        it('Should generate an UPDATE statement for one row', function() {
            const sql = mysqlDaoQueryHelper.updateOne({id: 1}, {email: 'john.doe@gmail.com', firstName: 'john'});
            Should.exist(sql);

            sql.should.eql("UPDATE users SET email = 'john.doe@gmail.com', first_name = 'john' WHERE (id = 1) LIMIT 1");
        });

        it('Should generate an UPDATE statement and not escape CURRENT_TIMESTAMP', function() {
            const sql = mysqlDaoQueryHelper.updateOne({id: 1}, {email: 'john.doe@gmail.com', dateDeleted: 'CURRENT_TIMESTAMP'});
            Should.exist(sql);

            sql.should.eql("UPDATE users SET email = 'john.doe@gmail.com', date_deleted = CURRENT_TIMESTAMP WHERE (id = 1) LIMIT 1");
        });

    });

    describe('deleteOne', function() {

        it('Should generate an DELETE statement for one row', function() {
            const sql = mysqlDaoQueryHelper.deleteOne({id: 1});
            Should.exist(sql);

            sql.should.eql("DELETE FROM users WHERE (id = 1) LIMIT 1");
        });

    });

    describe('_cleanAndMapValues', function() {

        it('Should convert camelCase property names to snakecase property names', function() {
            const output = mysqlDaoQueryHelper._cleanAndMapValues({firstName: 'John', lastName: 'Doe'});
            Should.exist(output);
            output.should.have.properties(['first_name','last_name']);
            output.first_name.should.eql("'John'");
            output.last_name.should.eql("'Doe'");
        });

        it('Should converts a VO to JS object and cleans that', function() {
            const input = new UserCreateArgs({firstName: 'firstName', lastName: 'lastName', password: 'boom!'});
            const output = mysqlDaoQueryHelper._cleanAndMapValues(input);
            Should.exist(output);
            output.should.have.properties(['first_name','last_name','password']);
            output.first_name.should.eql("'firstName'");
            output.last_name.should.eql("'lastName'");
            output.password.should.eql("'boom!'");
        });

        it('Should converts an object with toJsObj defined cleans that', function() {
            const output = mysqlDaoQueryHelper._cleanAndMapValues({
                toJsObj: function() {
                    return {firstName: 'firstName', lastName: 'lastName', password: 'boom!'};
                }
            });
            Should.exist(output);
            output.should.have.properties(['first_name','last_name','password']);
            output.first_name.should.eql("'firstName'");
            output.last_name.should.eql("'lastName'");
            output.password.should.eql("'boom!'");
        });

        it('Should converts an object with toJSON defined cleans that', function() {
            const output = mysqlDaoQueryHelper._cleanAndMapValues({
                toJSON: function() {
                    return {firstName: 'John', lastName: 'Doe', password: 'another'};
                }
            });
            Should.exist(output);
            output.should.have.properties(['first_name','last_name','password']);
            output.first_name.should.eql("'John'");
            output.last_name.should.eql("'Doe'");
            output.password.should.eql("'another'");
        });

        it('Drops arrays and objects', function() {
            const output = mysqlDaoQueryHelper._cleanAndMapValues({
                firstName: 'firstName',
                lastName: 'lastName',
                password: 'boom!',
                wontBeInOutput: [],
                alsoWontBeThere: {}
            });
            Should.exist(output);
            output.should.have.properties(['first_name','last_name','password']);
            output.should.not.have.properties(['wont_be_in_output','also_wont_be_there']);
            output.first_name.should.eql("'firstName'");
            output.last_name.should.eql("'lastName'");
            output.password.should.eql("'boom!'");
        });

        it('Allows special value CURRENT_TIMESTAMP and does not escape with quotes', function() {
            const output = mysqlDaoQueryHelper._cleanAndMapValues({
                firstName: 'firstName',
                dateCreated: 'CURRENT_TIMESTAMP',
                dateAgain: 'NOW()'
            });
            Should.exist(output);
            output.should.have.properties(['first_name','date_created']);
            output.first_name.should.eql("'firstName'");
            output.date_created.should.eql("CURRENT_TIMESTAMP");
            output.date_again.should.eql("NOW()");
        });

        it('Escapes special value CURRENT_TIMESTAMP when explicitly asked', function() {
            const output = mysqlDaoQueryHelper._cleanAndMapValues({
                firstName: 'firstName',
                dateCreated: 'CURRENT_TIMESTAMP',
                dateAgain: 'NOW()'
            }, {dontCleanMysqlFunctions: false});
            Should.exist(output);
            output.should.have.properties(['first_name','date_created']);
            output.first_name.should.eql("'firstName'");
            output.date_created.should.eql("'CURRENT_TIMESTAMP'");
            output.date_again.should.eql("'NOW()'");
        });

        it('Convert undefined value to null', function() {
            const someObj = {firstName: 'John', lastName: 'Doe'};
            const output = mysqlDaoQueryHelper._cleanAndMapValues({
                firstName: someObj.firstName,
                lastName: someObj.lastName,
                password: someObj.password
            });
            Should.exist(output);
            output.should.have.properties(['first_name','last_name','password']);
            output.first_name.should.eql("'John'");
            output.last_name.should.eql("'Doe'");
            output.password.should.eql("NULL");
        });

    });

    describe('_appendWhereClause', function() {

        it('Should generate good WHERE clause for standard object input', function() {
            const sqlObj = Squel.select().from('users');

            mysqlDaoQueryHelper._appendWhereClause(sqlObj, {firstName: 'John', lastName: 'Doe'});

            const sqlString = sqlObj.toString();

            sqlString.should.eql("SELECT * FROM users WHERE (first_name = 'John') AND (last_name = 'Doe')");
        });

        it('Should generate good WHERE clause when input contains NULL value', function() {
            const sqlObj = Squel.select().from('users');

            mysqlDaoQueryHelper._appendWhereClause(sqlObj, {firstName: 'John', lastName: null});

            const sqlString = sqlObj.toString();

            sqlString.should.eql("SELECT * FROM users WHERE (first_name = 'John') AND (last_name IS NULL)");
        });

        it('Should generate good WHERE clause when input contains undefined value (assumes it means NULL)', function() {
            const sqlObj = Squel.select().from('users');

            const someObj = {firstName: 'John'};
            mysqlDaoQueryHelper._appendWhereClause(sqlObj, {firstName: someObj.firstName, lastName: someObj.lastName});

            const sqlString = sqlObj.toString();

            sqlString.should.eql("SELECT * FROM users WHERE (first_name = 'John') AND (last_name IS NULL)");
        });

    });

});
