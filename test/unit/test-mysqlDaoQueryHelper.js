'use strict';

const Should = require('should');
const Mysql = require('mysql');
const Squel = require('squel');
const VO = require('hapiest-vo');
const Moment = require('moment');

const MysqlDaoQueryHelper = require('../../lib/mysqlDaoQueryHelper');

const cleanFunction = value => Mysql.escape(value, false, 'utc');
const mysqlDaoQueryHelper = new MysqlDaoQueryHelper('users', cleanFunction);

const localTimezoneCleanFunction = value => Mysql.escape(value);
const localMysqlDaoQueryHelper = new MysqlDaoQueryHelper('users', localTimezoneCleanFunction);

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
            const sql = mysqlDaoQueryHelper.create({firstName: 'firstName', lastName: 'lastName', password: 'boom!', dateAdded: new Date('1990-01-05T13:30:00Z')});
            Should.exist(sql);

            sql.should.eql(`INSERT INTO users (first_name, last_name, password, date_added) VALUES ('firstName', 'lastName', 'boom!', '1990-01-05 13:30:00.000')`);
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

        it('Should generate dates with for a utc timezone ', function() {
            const now = Moment();
            const nowDate = now.toDate();
            const utcDateString = now.utc().format('YYYY-MM-DD HH:mm:ss.SSS');

            const sql = mysqlDaoQueryHelper.getOne({dateAdded: nowDate});
            Should.exist(sql);

            sql.should.eql(`SELECT * FROM users WHERE (date_added = '${utcDateString}') LIMIT 1`);
        });

        it('Should generate dates with for local timezone ', function() {
            const now = Moment();
            const nowDate = now.toDate();
            const localDateString = now.local().format('YYYY-MM-DD HH:mm:ss.SSS');

            const sql = localMysqlDaoQueryHelper.getOne({dateAdded: nowDate});
            Should.exist(sql);

            sql.should.eql(`SELECT * FROM users WHERE (date_added = '${localDateString}') LIMIT 1`);
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

        it('Should allow an array to be passed to WHERE clause', function() {
            const sql = mysqlDaoQueryHelper.getAll({id: [1,2,5,10]});
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (id IN (1,2,5,10))");
        });

        it('Should generate a SELECT statement with order by', function() {
            const sql = mysqlDaoQueryHelper.getAll({}, { sort: { firstName: 'ASC', lastName: 'DESC' } });
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users ORDER BY first_name ASC, last_name DESC");
        });

        it('Should generate a SELECT statement with limit', function() {
            const sql = mysqlDaoQueryHelper.getAll({}, { limit: 2 });
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users LIMIT 2");
        });

        it('Should generate a SELECT statement with offset and limit', function() {
            const sql = mysqlDaoQueryHelper.getAll({}, { offset: 1, limit: 2 });
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users LIMIT 2 OFFSET 1");
        });

        it('Should generate a SELECT statement with a query object and gt/lt', function() {
            const sql = mysqlDaoQueryHelper.getAll({
                dateAdded: {
                    gte: new Date('1990-01-05T13:30:00Z'),
                    lt: new Date('1990-01-10T13:30:00Z')
                }
            });
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (date_added >= '1990-01-05 13:30:00.000' AND date_added < '1990-01-10 13:30:00.000')");
        });

        it('Should generate a SELECT statement with a query object and gte/lte', function() {
            const sql = mysqlDaoQueryHelper.getAll({
                id: {
                    gte: 5,
                    lte: 10
                }
            });
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (id >= 5 AND id <= 10)");
        });

        it('Should generate a SELECT statement with multiple query objects', function() {
            const sql = mysqlDaoQueryHelper.getAll({
                name: {
                    eq: 'Chas'
                },
                lastName: {
                    ne: 'Fantastic'
                },
                id: {
                    gt: 2
                },
                dateAdded: {
                    gt: new Date('1990-01-05T13:30:00Z'),
                    lte: new Date('1990-01-10T13:30:00Z')
                }
            });
            Should.exist(sql);

            sql.should.eql("SELECT * FROM users WHERE (name = 'Chas') AND (last_name != 'Fantastic') AND (id > 2) AND (date_added > '1990-01-05 13:30:00.000' AND date_added <= '1990-01-10 13:30:00.000')");
        });

    });

    describe('getCount', function() {
        it('Should generate a SELECT count(*) statement', function() {
            const sql = mysqlDaoQueryHelper.getCount({lastName: 'Doe'});
            Should.exist(sql);

            sql.should.eql("SELECT COUNT(*) AS \"count\" FROM users WHERE (last_name = 'Doe')");
        });

        it('Should generate a SELECT count(*) statement with limit', function() {
            const sql = mysqlDaoQueryHelper.getCount({lastName: 'Doe'}, { limit: 2 });
            Should.exist(sql);

            sql.should.eql("SELECT COUNT(*) AS \"count\" FROM users WHERE (last_name = 'Doe')");
        });

        it('Should generate a SELECT count(*) statement with limit and offset', function() {
            const sql = mysqlDaoQueryHelper.getCount({lastName: 'Doe'}, { offset: 1, limit: 2 });
            Should.exist(sql);

            sql.should.eql("SELECT COUNT(*) AS \"count\" FROM users WHERE (last_name = 'Doe')");
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

        it('Should work even with ? in WHERE clause for one row', function() {
            const sql = mysqlDaoQueryHelper.updateOne({url: 'http://some.com/?blah=hello'}, {email: 'john.doe@gmail.com', firstName: 'john'});
            Should.exist(sql);

            sql.should.eql("UPDATE users SET email = 'john.doe@gmail.com', first_name = 'john' WHERE (url = 'http://some.com/?blah=hello') LIMIT 1");
        });

    });

    describe('update', function() {

        it('Should generate an UPDATE statement that has no LIMIT clause', function() {
            const sql = mysqlDaoQueryHelper.update({id: 1}, {email: 'john.doe@gmail.com', firstName: 'john', dateUpdated: new Date('2016-09-27T11:30:00Z')});
            Should.exist(sql);

            sql.should.eql("UPDATE users SET email = 'john.doe@gmail.com', first_name = 'john', date_updated = '2016-09-27 11:30:00.000' WHERE (id = 1)");
        })

    });

    describe('deleteOne', function() {

        it('Should generate an DELETE statement for one row', function() {
            const sql = mysqlDaoQueryHelper.deleteOne({id: 1});
            Should.exist(sql);

            sql.should.eql("DELETE FROM users WHERE (id = 1) LIMIT 1");
        });

        it('Should generate an DELETE statement even with ? in WHERE clause for one row', function() {
            const sql = mysqlDaoQueryHelper.deleteOne({url: 'http://some.com/?foo=bar'});
            Should.exist(sql);

            sql.should.eql("DELETE FROM users WHERE (url = 'http://some.com/?foo=bar') LIMIT 1");
        });

    });

    describe('delete', function() {

        it('Should generate an DELETE statement that has no LIMIT clause', function() {
            const sql = mysqlDaoQueryHelper.delete({age: 30});
            Should.exist(sql);

            sql.should.eql("DELETE FROM users WHERE (age = 30)");
        })

    });

    describe('clean', function() {
        it('Should clean value passed in', function() {
            const uncleanValue = "some unclean value with ' single quote";
            const cleanValue = mysqlDaoQueryHelper.clean(uncleanValue);

            Should.exist(cleanValue);
            cleanValue.should.eql("'some unclean value with \\' single quote'");
        });

        it('Should escape all strings, even special MySQL functions', function() {
            const currentTimestampUnclean = "current_timestamp";
            const currentTimestampClean = mysqlDaoQueryHelper.clean(currentTimestampUnclean);

            Should.exist(currentTimestampClean);
            currentTimestampClean.should.eql("'current_timestamp'");

            const nowUnclean = "NOW()";
            const nowClean = mysqlDaoQueryHelper.clean(nowUnclean);

            Should.exist(nowClean);
            nowClean.should.eql("'NOW()'");

            const isNullUnclean = "IS NULL";
            const isNullClean = mysqlDaoQueryHelper.clean(isNullUnclean);

            Should.exist(isNullClean);
            isNullClean.should.eql("'IS NULL'");

            const isNotNullUnclean = "IS NOT NULL";
            const isNotNullClean = mysqlDaoQueryHelper.clean(isNotNullUnclean);

            Should.exist(isNotNullClean);
            isNotNullClean.should.eql("'IS NOT NULL'");
        });

        it('Should clean a date configured for utc time', function() {
            const now = Moment();
            const date = now.toDate();
            const dateClean = mysqlDaoQueryHelper.clean(date);
            const expectedDateString = now.utc().format('YYYY-MM-DD HH:mm:ss.SSS');

            Should.exist(dateClean);
            dateClean.should.eql(`'${expectedDateString}'`);
        });

        it('Should clean a date configured for local time', function() {
            const now = Moment();
            const date = now.toDate();
            const dateClean = localMysqlDaoQueryHelper.clean(date);
            const expectedDateString = now.local().format('YYYY-MM-DD HH:mm:ss.SSS');

            Should.exist(dateClean);
            dateClean.should.eql(`'${expectedDateString}'`);
        })
    });

    describe('cleanSpecial', function() {
        it('Should clean value passed in', function() {
            const uncleanValue = "some unclean value with ' single quote";
            const cleanValue = mysqlDaoQueryHelper.cleanSpecial(uncleanValue);

            Should.exist(cleanValue);
            cleanValue.should.eql("'some unclean value with \\' single quote'");
        });

        it('Should allow special MySQL functions to pass through uncleaned', function() {
            const currentTimestampUnclean = "current_timestamp";
            const currentTimestampClean = mysqlDaoQueryHelper.cleanSpecial(currentTimestampUnclean);

            Should.exist(currentTimestampClean);
            currentTimestampClean.should.eql("current_timestamp");

            const nowUnclean = "NOW()";
            const nowClean = mysqlDaoQueryHelper.cleanSpecial(nowUnclean);

            Should.exist(nowClean);
            nowClean.should.eql("NOW()");

            const isNullUnclean = "IS NULL";
            const isNullClean = mysqlDaoQueryHelper.cleanSpecial(isNullUnclean);

            Should.exist(isNullClean);
            isNullClean.should.eql("IS NULL");

            const isNotNullUnclean = "IS NOT NULL";
            const isNotNullClean = mysqlDaoQueryHelper.cleanSpecial(isNotNullUnclean);

            Should.exist(isNotNullClean);
            isNotNullClean.should.eql("IS NOT NULL");
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

        it('Drops empty arrays and objects', function() {
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

        it('Escapes primitive values in arrays', function() {
            const output = mysqlDaoQueryHelper._cleanAndMapValues({
                firstName: 'firstName',
                lastName: 'lastName',
                password: 'boom!',
                idArray: [1,2,{},"'bob",3,4]
            });
            Should.exist(output);
            output.should.have.properties(['first_name','last_name','password','id_array']);
            output.first_name.should.eql("'firstName'");
            output.last_name.should.eql("'lastName'");
            output.password.should.eql("'boom!'");
            output.id_array.should.deepEqual(['1','2',"\'\\\'bob\'",'3','4'])
        });

        it('Allows special values (CURRENT_TIMESTAMP, NOW(), IS NULL, IS NOT NULL) and does not escape with quotes', function() {
            const output = mysqlDaoQueryHelper._cleanAndMapValues({
                firstName: 'firstName',
                dateCreated: 'CURRENT_TIMESTAMP',
                dateAgain: 'NOW()',
                someNullField: 'IS NULL',
                nonNullField: 'IS NOT NULL'
            });
            Should.exist(output);
            output.should.have.properties(['first_name','date_created']);
            output.first_name.should.eql("'firstName'");
            output.date_created.should.eql("CURRENT_TIMESTAMP");
            output.date_again.should.eql("NOW()");
            output.some_null_field.should.eql("IS NULL");
            output.non_null_field.should.eql("IS NOT NULL");
        });

        it('Escapes special values (CURRENT_TIMESTAMP, NOW(), IS NULL, IS NOT NULL) when explicitly asked', function() {
            const output = mysqlDaoQueryHelper._cleanAndMapValues({
                firstName: 'firstName',
                dateCreated: 'CURRENT_TIMESTAMP',
                dateAgain: 'NOW()',
                someNullField: 'IS NULL',
                nonNullField: 'IS NOT NULL'
            }, {dontCleanMysqlFunctions: false});
            Should.exist(output);
            output.should.have.properties(['first_name','date_created']);
            output.first_name.should.eql("'firstName'");
            output.date_created.should.eql("'CURRENT_TIMESTAMP'");
            output.date_again.should.eql("'NOW()'");
            output.some_null_field.should.eql("'IS NULL'");
            output.non_null_field.should.eql("'IS NOT NULL'");
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

        it('Should generate good WHERE clause when input contains special functions', function() {
            const sqlObj = Squel.select().from('users');

            const someObj = {firstName: 'John'};
            mysqlDaoQueryHelper._appendWhereClause(sqlObj, {dateCreated: 'CURRENT_TIMESTAMP', dateUpdated: 'NOW()', email: 'IS NULL', firstName: 'IS NOT NULL'});

            const sqlString = sqlObj.toString();

            sqlString.should.eql("SELECT * FROM users WHERE (date_created = CURRENT_TIMESTAMP) AND (date_updated = NOW()) AND (email IS NULL) AND (first_name IS NOT NULL)");
        });

    });

});
