'use strict';

const Should = require('should');
const _ = require('lodash');
const VO = require('hapiest-vo');
const Sinon = require('sinon');

const MysqlDao = require('../../lib/mysqlDao');
const MysqlDaoArgsFactory = require('../../lib/mysqlDaoArgsFactory');
const MysqlServiceFactory = require('../../lib/mysqlServiceFactory');

class User extends VO {
    constructor(args) {
        super();
        this._addProperties(args);
    }

    get id() { return this.get('id'); }
    get firstName() { return this.get('firstName'); }
    get lastName() { return this.get('lastName'); }
    get email() { return this.get('email'); }
    get dateCreated() { return this.get('dateCreated'); }
}

class UserCreateArgs extends VO {
    constructor(args) {
        super();
        this._addProperties(args);
    }

    get firstName() { return this.get('firstName'); }
    get lastName() { return this.get('lastName'); }
    get email() { return this.get('email'); }
}

const createUserFromDbRow = (dbRow) => {
    const userArgs = {};
    Object.keys(dbRow).forEach(columnName => {
        const camelCaseColumn = _.camelCase(columnName);
        userArgs[camelCaseColumn] = dbRow[columnName];
    });
    return new User(userArgs);
};

const LoggerFactory = require('hapiest-logger/lib/loggerFactory');
const LoggerConfigFactory = require('hapiest-logger/lib/loggerConfigFactory');
const loggerConfig = LoggerConfigFactory.createFromJsObj({enabled: true, consoleTransport: {enabled:true, level: 'info'}});
const logger = LoggerFactory.createLogger(loggerConfig);


// @TODO: moves this elsewhere perhaps
const writeConnectionConfig = {
    host: 'localhost',
    database: 'hapiestmysql',
    user: 'hapiestmysql',
    password: 'hapiestmysql',
    connectionLimit: 1
};
const mysqlService = MysqlServiceFactory.createFromObjWithOnePool(writeConnectionConfig, logger);

const mysqlDaoArgs = MysqlDaoArgsFactory.createFromJsObj({
    mysqlService: mysqlService,
    createVoFromDbRowFunction: createUserFromDbRow,
    logger: logger
});

class UserDao extends MysqlDao {
    get tableName() {return 'users';}
}
const userDao = new UserDao(mysqlDaoArgs);

function databaseSetup(done) {

    const queries = [
        'DROP TABLE IF EXISTS users',
        `
                CREATE TABLE IF NOT EXISTS users (
                    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    first_name VARCHAR(100) NULL,
                    last_name VARCHAR(100) NULL,
                    email VARCHAR(255) NOT NULL,
                    date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            `
    ];

    mysqlService.executeQueries(queries)
        .then(() => done(), (err) => done(err));
}

function databaseTeardown(done) {
    const queries = ['DROP TABLE IF EXISTS users'];
    mysqlService.executeQueries(queries)
        .then(() => done(), (err) => done(err));
}


describe('MysqlDao', function() {

    after(databaseTeardown);

    describe('create', function() {
        beforeEach(databaseSetup);

        it('Should create a single row in the users table', function() {
            return userDao.create({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'})
                .then(id => {
                    Should.exist(id);
                    id.should.be.a.Number();

                    const checkRowPromise = mysqlService.selectOne(`SELECT * FROM users WHERE email = 'john.doe@gmail.com'`)
                        .then(dbRow => {
                            Should.exist(dbRow);
                            dbRow.id.should.be.a.Number();
                            dbRow.first_name.should.eql('John');
                            dbRow.last_name.should.eql('Doe');
                            dbRow.email.should.eql('john.doe@gmail.com');
                            Should.exist(dbRow.date_created);
                        });
                    return Promise.all([checkRowPromise]);
                });
        });

        it('Should create a single row in the users table from a VO', function() {
            return userDao.create(new UserCreateArgs({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'}))
                .then(id => {
                    Should.exist(id);
                    id.should.be.a.Number();

                    const checkRowPromise = mysqlService.selectOne(`SELECT * FROM users WHERE email = 'john.doe@gmail.com'`)
                        .then(dbRow => {
                            Should.exist(dbRow);
                            dbRow.id.should.be.a.Number();
                            dbRow.first_name.should.eql('John');
                            dbRow.last_name.should.eql('Doe');
                            dbRow.email.should.eql('john.doe@gmail.com');
                            Should.exist(dbRow.date_created);
                        });
                    return Promise.all([checkRowPromise]);
                });
        });

    });

    describe('createBulk', function() {
        beforeEach(databaseSetup);

        it('Should create two row in the users table', function() {
            return userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ])
                .then((numRows) => {
                    Should.exist(numRows);
                    numRows.should.eql(2);

                    const checkRowPromise1 = mysqlService.selectOne(`SELECT * FROM users WHERE email = 'john.doe@gmail.com'`)
                        .then(dbRow => {
                            Should.exist(dbRow);
                            dbRow.id.should.be.a.Number();
                            dbRow.first_name.should.eql('John');
                            dbRow.last_name.should.eql('Doe');
                            dbRow.email.should.eql('john.doe@gmail.com');
                            Should.exist(dbRow.date_created);
                        });

                    const checkRowPromise2 = mysqlService.selectOne(`SELECT * FROM users WHERE email = 'jane.doe@gmail.com'`)
                        .then(dbRow => {
                            Should.exist(dbRow);
                            dbRow.id.should.be.a.Number();
                            dbRow.first_name.should.eql('Jane');
                            dbRow.last_name.should.eql('Doe');
                            dbRow.email.should.eql('jane.doe@gmail.com');
                            Should.exist(dbRow.date_created);
                        });

                    return Promise.all([checkRowPromise1, checkRowPromise2]);
                });
        });
    });

    describe('getOneById', function() {
        beforeEach(databaseSetup);

        it('Should fetch a single row by id', function() {
            let newId = null;
            return userDao.create({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'})
                .then(id => { newId = id})
                .then(() => {
                    const createPromise = userDao.create({firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}); // Add this to make sure the ID lookup actually does something
                    return Promise.all([createPromise]);
                })
                .then(() => {
                    const checkRowPromise = userDao.getOneById(newId)
                        .then(user => {
                            Should.exist(user);
                            user.should.be.an.instanceOf(User);
                            user.id.should.be.a.Number();
                            user.firstName.should.eql('John');
                            user.lastName.should.eql('Doe');
                            user.email.should.eql('john.doe@gmail.com');
                            Should.exist(user.dateCreated);
                        });
                    return Promise.all([checkRowPromise]);
                });
        });

    });

    describe('getOneByIdFromMaster', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.selectOneFromMaster.restore) {
                userDao._mysqlService.selectOneFromMaster.restore();
            }
        });

        it('Should fetch a single row by id', function() {
            let newId = null;
            const selectOneFromMasterSpy = Sinon.spy(userDao._mysqlService, 'selectOneFromMaster');
            return userDao.create({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'})
                .then(id => { newId = id})
                .then(() => {
                    const createPromise = userDao.create({firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}); // Add this to make sure the ID lookup actually does something
                    return Promise.all([createPromise]);
                })
                .then(() => {
                    const checkRowPromise = userDao.getOneByIdFromMaster(newId)
                        .then(user => {
                            Should.exist(user);
                            user.should.be.an.instanceOf(User);
                            user.id.should.be.a.Number();
                            user.firstName.should.eql('John');
                            user.lastName.should.eql('Doe');
                            user.email.should.eql('john.doe@gmail.com');
                            Should.exist(user.dateCreated);

                            selectOneFromMasterSpy.calledOnce.should.be.True();
                        });
                    return Promise.all([checkRowPromise]);
                });
        });

    });

    describe('getOne', function() {
        beforeEach(databaseSetup);

        it('Should fetch a single row by email and first name', function() {
            let newId = null;
            return userDao.create({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'})
                .then(id => { newId = id})
                .then(() => {
                    const createPromise = userDao.create({firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}); // Add this to make sure the ID lookup actually does something
                    return Promise.all([createPromise]);
                })
                .then(() => {
                    const checkRowPromise = userDao.getOne({firstName: 'Jane', email: 'jane.doe@gmail.com'})
                        .then(user => {
                            Should.exist(user);
                            user.should.be.an.instanceOf(User);
                            user.id.should.be.a.Number();
                            user.firstName.should.eql('Jane');
                            user.lastName.should.eql('Doe');
                            user.email.should.eql('jane.doe@gmail.com');
                            Should.exist(user.dateCreated);
                        });
                    return Promise.all([checkRowPromise]);
                });
        });
    });

    describe('getOneFromMaster', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.selectOneFromMaster.restore) {
                userDao._mysqlService.selectOneFromMaster.restore();
            }
        });

        it('Should fetch a single row by email and first name', function() {
            let newId = null;
            const selectOneFromMasterSpy = Sinon.spy(userDao._mysqlService, 'selectOneFromMaster');
            return userDao.create({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'})
                .then(id => { newId = id})
                .then(() => {
                    const createPromise = userDao.create({firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}); // Add this to make sure the ID lookup actually does something
                    return Promise.all([createPromise]);
                })
                .then(() => {
                    const checkRowPromise = userDao.getOneFromMaster({firstName: 'Jane', email: 'jane.doe@gmail.com'})
                        .then(user => {
                            Should.exist(user);
                            user.should.be.an.instanceOf(User);
                            user.id.should.be.a.Number();
                            user.firstName.should.eql('Jane');
                            user.lastName.should.eql('Doe');
                            user.email.should.eql('jane.doe@gmail.com');
                            Should.exist(user.dateCreated);

                            selectOneFromMasterSpy.calledOnce.should.be.True();
                        });
                    return Promise.all([checkRowPromise]);
                });
        });
    });

    describe('getOneFromSql', function() {
        beforeEach(databaseSetup);

        it('Should fetch a single row by email and first name with only email and first_name return in the results', function() {
            let newId = null;
            return userDao.create({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'})
                .then(id => { newId = id})
                .then(() => {
                    const createPromise = userDao.create({firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}); // Add this to make sure the ID lookup actually does something
                    return Promise.all([createPromise]);
                })
                .then(() => {
                    const sql = "SELECT email, first_name FROM users WHERE first_name='Jane' AND email='jane.doe@gmail.com'";
                    const checkRowPromise = userDao.getOneFromSql(sql)
                        .then(user => {
                            Should.exist(user);
                            Should.not.exist(user.id);
                            Should.not.exist(user.lastName);
                            Should.not.exist(user.dateCreated);

                            Should.exist(user.email);
                            Should.exist(user.firstName);

                            user.email.should.eql('jane.doe@gmail.com');
                            user.firstName.should.eql('Jane');
                        });
                    return Promise.all([checkRowPromise]);
                });
        });
    });

    describe('getOneFromSqlRaw', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.restore) {
                userDao._mysqlService.restore();
            }
        });

        it('Should fetch a single row by email and first name with only email and first_name return in the results', function() {
            let newId = null;
            return userDao.create({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'})
                .then(id => { newId = id})
                .then(() => {
                    const createPromise = userDao.create({firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}); // Add this to make sure the ID lookup actually does something
                    return Promise.all([createPromise]);
                })
                .then(() => {
                    const sql = "SELECT email, first_name FROM users WHERE first_name='Jane' AND email='jane.doe@gmail.com'";
                    const checkRowPromise = userDao.getOneFromSqlRaw(sql)
                        .then(user => {
                            Should.exist(user);
                            Should.not.exist(user.id);
                            Should.not.exist(user.lastName);
                            Should.not.exist(user.dateCreated);

                            Should.exist(user.email);
                            Should.exist(user.first_name);

                            user.email.should.eql('jane.doe@gmail.com');
                            user.first_name.should.eql('Jane');
                        });
                    return Promise.all([checkRowPromise]);
                });
        });
    });

    describe('getOneFromSqlFromMaster', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.selectOneFromMaster.restore) {
                userDao._mysqlService.selectOneFromMaster.restore();
            }
        });

        it('Should fetch a single row by email and first name with only email and first_name return in the results', function() {
            let newId = null;
            const selectOneFromMasterSpy = Sinon.spy(userDao._mysqlService, 'selectOneFromMaster');
            return userDao.create({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'})
                .then(id => { newId = id})
                .then(() => {
                    const createPromise = userDao.create({firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}); // Add this to make sure the ID lookup actually does something
                    return Promise.all([createPromise]);
                })
                .then(() => {
                    const sql = "SELECT email, first_name FROM users WHERE first_name='Jane' AND email='jane.doe@gmail.com'";
                    const checkRowPromise = userDao.getOneFromSqlFromMaster(sql)
                        .then(user => {
                            Should.exist(user);
                            Should.not.exist(user.id);
                            Should.not.exist(user.lastName);
                            Should.not.exist(user.dateCreated);

                            Should.exist(user.email);
                            Should.exist(user.firstName);

                            user.email.should.eql('jane.doe@gmail.com');
                            user.firstName.should.eql('Jane');

                            selectOneFromMasterSpy.calledOnce.should.be.True();
                        });
                    return Promise.all([checkRowPromise]);
                });
        });
    });

    describe('getOneFromSqlFromMasterRaw', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.selectOneFromMaster.restore) {
                userDao._mysqlService.selectOneFromMaster.restore();
            }
        });

        it('Should fetch a single row by email and first name with only email and first_name return in the results', function() {
            let newId = null;
            const selectOneFromMasterSpy = Sinon.spy(userDao._mysqlService, 'selectOneFromMaster');
            return userDao.create({firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'})
                .then(id => { newId = id})
                .then(() => {
                    const createPromise = userDao.create({firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}); // Add this to make sure the ID lookup actually does something
                    return Promise.all([createPromise]);
                })
                .then(() => {
                    const sql = "SELECT email, first_name FROM users WHERE first_name='Jane' AND email='jane.doe@gmail.com'";
                    const checkRowPromise = userDao.getOneFromSqlFromMasterRaw(sql)
                        .then(user => {
                            Should.exist(user);
                            Should.not.exist(user.id);
                            Should.not.exist(user.lastName);
                            Should.not.exist(user.dateCreated);

                            Should.exist(user.email);
                            Should.exist(user.first_name);

                            user.email.should.eql('jane.doe@gmail.com');
                            user.first_name.should.eql('Jane');

                            selectOneFromMasterSpy.calledOnce.should.be.True();
                        });
                    return Promise.all([checkRowPromise]);
                });
        });
    });

    describe('getAll', function() {
        beforeEach(databaseSetup);

        it('Should return two users', function() {
            return userDao.createBulk([
                    {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                    {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
                ])
                .then((numRows) => {
                    Should.exist(numRows);
                    numRows.should.eql(2);
                    const checkRowPromise1 = userDao.getAll({lastName: 'Doe'})
                        .then(users => {
                            Should.exist(users);
                            users.should.be.an.Array();
                            users.length.should.eql(2);

                            users[0].should.be.an.instanceOf(User);
                            users[0].firstName.should.eql('John');
                            users[0].lastName.should.eql('Doe');
                            users[0].email.should.eql('john.doe@gmail.com');

                            users[1].should.be.an.instanceOf(User);
                            users[1].firstName.should.eql('Jane');
                            users[1].lastName.should.eql('Doe');
                            users[1].email.should.eql('jane.doe@gmail.com');
                        });

                    return Promise.all([checkRowPromise1]);
                });
        });
    });

    describe('getAllFromMaster', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.selectAllFromMaster.restore) {
                userDao._mysqlService.selectAllFromMaster.restore();
            }
        });

        it('Should return two users', function() {
            const selectAllFromMasterSpy = Sinon.spy(userDao._mysqlService, 'selectAllFromMaster');
            return userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ])
                .then((numRows) => {
                    Should.exist(numRows);
                    numRows.should.eql(2);
                    const checkRowPromise1 = userDao.getAllFromMaster({lastName: 'Doe'})
                        .then(users => {
                            Should.exist(users);
                            users.should.be.an.Array();
                            users.length.should.eql(2);

                            users[0].should.be.an.instanceOf(User);
                            users[0].firstName.should.eql('John');
                            users[0].lastName.should.eql('Doe');
                            users[0].email.should.eql('john.doe@gmail.com');

                            users[1].should.be.an.instanceOf(User);
                            users[1].firstName.should.eql('Jane');
                            users[1].lastName.should.eql('Doe');
                            users[1].email.should.eql('jane.doe@gmail.com');

                            selectAllFromMasterSpy.calledOnce.should.be.True();
                        });

                    return Promise.all([checkRowPromise1]);
                });
        });
    });

    describe('getAllFromSql', function() {
        beforeEach(databaseSetup);

        it('Should return two rows with only first_name populated', function() {
            return userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ])
                .then((numRows) => {
                    Should.exist(numRows);
                    numRows.should.eql(2);
                    const sql = "SELECT first_name FROM users WHERE last_name='Doe'";
                    const checkRowPromise1 = userDao.getAllFromSql(sql)
                        .then(dbRows => {
                            Should.exist(dbRows);
                            dbRows.should.be.an.Array();
                            dbRows.length.should.eql(2);

                            dbRows[0].firstName.should.eql('John');
                            dbRows[1].firstName.should.eql('Jane');

                            Should.not.exist(dbRows[0].email);
                            Should.not.exist(dbRows[0].lastName);
                            Should.not.exist(dbRows[1].email);
                            Should.not.exist(dbRows[1].lastName);
                        });

                    return Promise.all([checkRowPromise1]);
                });
        });
    });

    describe('getAllFromSqlFromMaster', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.selectAllFromMaster.restore) {
                userDao._mysqlService.selectAllFromMaster.restore();
            }
        });

        it('Should return two rows with only first_name populated', function() {
            const selectAllFromMasterSpy = Sinon.spy(userDao._mysqlService, 'selectAllFromMaster');
            return userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ])
                .then((numRows) => {
                    Should.exist(numRows);
                    numRows.should.eql(2);
                    const sql = "SELECT first_name FROM users WHERE last_name='Doe'";
                    const checkRowPromise1 = userDao.getAllFromSqlFromMaster(sql)
                        .then(dbRows => {
                            Should.exist(dbRows);
                            dbRows.should.be.an.Array();
                            dbRows.length.should.eql(2);

                            dbRows[0].firstName.should.eql('John');
                            dbRows[1].firstName.should.eql('Jane');

                            Should.not.exist(dbRows[0].email);
                            Should.not.exist(dbRows[0].lastName);
                            Should.not.exist(dbRows[1].email);
                            Should.not.exist(dbRows[1].lastName);

                            selectAllFromMasterSpy.calledOnce.should.be.True();
                        });

                    return Promise.all([checkRowPromise1]);
                });
        });
    });

    describe('getAllFromSqlRaw', function() {
        beforeEach(databaseSetup);

        it('Should return two rows with only first_name populated', function() {
            return userDao.createBulk([
                    {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                    {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
                ])
                .then((numRows) => {

                    const sql = "SELECT first_name FROM users WHERE last_name='Doe'";
                    const checkRowPromise1 = userDao.getAllFromSqlRaw(sql)
                        .then(dbRows => {
                            Should.exist(dbRows);
                            dbRows.should.be.an.Array();
                            dbRows.length.should.eql(2);

                            dbRows[0].first_name.should.eql('John');
                            dbRows[1].first_name.should.eql('Jane');

                            Should.not.exist(dbRows[0].email);
                            Should.not.exist(dbRows[0].firstName);
                            Should.not.exist(dbRows[0].lastName);
                            Should.not.exist(dbRows[1].email);
                            Should.not.exist(dbRows[1].firstName);
                            Should.not.exist(dbRows[1].lastName);
                        });

                    return Promise.all([checkRowPromise1]);
                });
        });
    });

    describe('getAllFromSqlFromMasterRaw', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.selectAllFromMaster.restore) {
                userDao._mysqlService.selectAllFromMaster.restore();
            }
        });

        it('Should return two rows with only first_name populated', function() {
            const selectAllFromMasterSpy = Sinon.spy(userDao._mysqlService, 'selectAllFromMaster');
            return userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ])
                .then((numRows) => {

                    const sql = "SELECT first_name FROM users WHERE last_name='Doe'";
                    const checkRowPromise1 = userDao.getAllFromSqlFromMasterRaw(sql)
                        .then(dbRows => {
                            Should.exist(dbRows);
                            dbRows.should.be.an.Array();
                            dbRows.length.should.eql(2);

                            dbRows[0].first_name.should.eql('John');
                            dbRows[1].first_name.should.eql('Jane');

                            Should.not.exist(dbRows[0].email);
                            Should.not.exist(dbRows[0].firstName);
                            Should.not.exist(dbRows[0].lastName);
                            Should.not.exist(dbRows[1].email);
                            Should.not.exist(dbRows[1].firstName);
                            Should.not.exist(dbRows[1].lastName);

                            selectAllFromMasterSpy.calledOnce.should.be.True();
                        });

                    return Promise.all([checkRowPromise1]);
                });
        });
    });

    describe('updateById', function() {
        beforeEach(databaseSetup);

        it('Should update Jane email address', function() {
            return userDao.createBulk([
                    {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                    {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
                ])
                .then((numRows) => {
                    const getIdPromise = userDao.getOne({email: 'jane.doe@gmail.com'}).then(user => user.id);
                    return Promise.all([getIdPromise]).then(results => results[0]);
                })
                .then(idToUpdate => {
                    const updatePromise = userDao.updateById(idToUpdate, {firstName: 'joe', lastName: 'bob'});
                    return Promise.all([updatePromise]).then(results => results[0]);
                })
                .then(numRowsChanged => {
                    Should.exist(numRowsChanged);
                    numRowsChanged.should.eql(1);

                    const assertPromise = userDao.getOne({email: 'jane.doe@gmail.com'})
                        .then(user => {
                            Should.exist(user);

                            user.firstName.should.eql('joe');
                            user.lastName.should.eql('bob');
                            user.email.should.eql('jane.doe@gmail.com');
                        });
                    return Promise.all([assertPromise]);
                });
        });
    });

    describe('updateOne', function() {
        beforeEach(databaseSetup);

        it('Should update John Doe user', function() {
            return userDao.createBulk([
                    {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                    {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
                ])
                .then(() => {
                    const updatePromise = userDao.updateOne({firstName: 'John'}, {firstName: 'joe', lastName: 'bob'});
                    return Promise.all([updatePromise]).then(results => results[0]);
                })
                .then(numRowsChanged => {
                    Should.exist(numRowsChanged);
                    numRowsChanged.should.eql(1);

                    const assertPromise = userDao.getOne({email: 'john.doe@gmail.com'})
                        .then(user => {
                            Should.exist(user);

                            user.firstName.should.eql('joe');
                            user.lastName.should.eql('bob');
                            user.email.should.eql('john.doe@gmail.com');
                        });
                    return Promise.all([assertPromise]);
                });
        });

    });

    describe('updateMultiple', function() {
        beforeEach(databaseSetup);

        it('Should update both John and Jane Doe users', function() {
            return userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ])
                .then(() => userDao.updateMultiple({lastName: 'Doe'}, {firstName: 'joe', lastName: 'bob'}))
                .then(numRowsChanged => {
                    Should.exist(numRowsChanged);
                    numRowsChanged.should.eql(2);

                    return userDao.getAll({firstName: 'joe', lastName: 'bob'})
                })
                .then(users => {
                    Should.exist(users);
                    users.length.should.eql(2);
                });
        });

    });

    describe('deleteById', function() {
        beforeEach(databaseSetup);

        it('Should delete Jane Doe user', function() {
            return userDao.createBulk([
                    {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                    {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
                ])
                .then((numRows) => {
                    const getIdPromise = userDao.getOne({email: 'jane.doe@gmail.com'}).then(user => user.id);
                    return Promise.all([getIdPromise]).then(results => results[0]);
                })
                .then(idToDelete => {
                    const deletePromise = userDao.deleteById(idToDelete);
                    return Promise.all([deletePromise]).then(results => results[0]);
                })
                .then(numRowsChanged => {
                    Should.exist(numRowsChanged);
                    numRowsChanged.should.eql(1);

                    const assertPromise = userDao.getOne({email: 'jane.doe@gmail.com'})
                        .then(user => {
                            Should.not.exist(user);
                        });
                    return Promise.all([assertPromise]);
                });
        });
    });

    describe('deleteOne', function() {
        beforeEach(databaseSetup);

        it('Should delete John Doe user', function() {
            return userDao.createBulk([
                    {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                    {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
                ])
                .then(idToDelete => {
                    const deletePromise = userDao.deleteOne({firstName: 'John'});
                    return Promise.all([deletePromise]).then(results => results[0]);
                })
                .then(numRowsChanged => {
                    Should.exist(numRowsChanged);
                    numRowsChanged.should.eql(1);

                    const assertPromise = userDao.getOne({email: 'john.doe@gmail.com'})
                        .then(user => {
                            Should.not.exist(user);
                        });
                    return Promise.all([assertPromise]);
                });
        });
    });

    describe('deleteMultiple', function() {
        beforeEach(databaseSetup);

        it('Should delete both John and Jane Doe users', function() {
            return userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ])
                .then(() => userDao.deleteMultiple({lastName: 'Doe'}))
                .then(numRowsChanged => {
                    Should.exist(numRowsChanged);
                    numRowsChanged.should.eql(2);

                    return userDao.getAll({})
                })
                .then(users => {
                    Should.exist(users);
                    users.length.should.eql(0);
                });
        });

    });

    describe('clean', function() {
        it('Should clean value passed in', function() {
            const uncleanValue = "some unclean value with ' single quote";
            const cleanValue = userDao.clean(uncleanValue);

            Should.exist(cleanValue);
            cleanValue.should.eql("'some unclean value with \\' single quote'");
        });

        it('Should escape all strings, even special MySQL functions', function() {
            const currentTimestampUnclean = "current_timestamp";
            const currentTimestampClean = userDao.clean(currentTimestampUnclean);

            Should.exist(currentTimestampClean);
            currentTimestampClean.should.eql("'current_timestamp'");

            const nowUnclean = "NOW()";
            const nowClean = userDao.clean(nowUnclean);

            Should.exist(nowClean);
            nowClean.should.eql("'NOW()'");

            const isNullUnclean = "IS NULL";
            const isNullClean = userDao.clean(isNullUnclean);

            Should.exist(isNullClean);
            isNullClean.should.eql("'IS NULL'");

            const isNotNullUnclean = "IS NOT NULL";
            const isNotNullClean = userDao.clean(isNotNullUnclean);

            Should.exist(isNotNullClean);
            isNotNullClean.should.eql("'IS NOT NULL'");
        });
    });

    describe('cleanSpecial', function() {
        it('Should clean value passed in', function() {
            const uncleanValue = "some unclean value with ' single quote";
            const cleanValue = userDao.cleanSpecial(uncleanValue);

            Should.exist(cleanValue);
            cleanValue.should.eql("'some unclean value with \\' single quote'");
        });

        it('Should allow special MySQL functions to pass through uncleaned', function() {
            const currentTimestampUnclean = "current_timestamp";
            const currentTimestampClean = userDao.cleanSpecial(currentTimestampUnclean);

            Should.exist(currentTimestampClean);
            currentTimestampClean.should.eql("current_timestamp");

            const nowUnclean = "NOW()";
            const nowClean = userDao.cleanSpecial(nowUnclean);

            Should.exist(nowClean);
            nowClean.should.eql("NOW()");

            const isNullUnclean = "IS NULL";
            const isNullClean = userDao.cleanSpecial(isNullUnclean);

            Should.exist(isNullClean);
            isNullClean.should.eql("IS NULL");

            const isNotNullUnclean = "IS NOT NULL";
            const isNotNullClean = userDao.cleanSpecial(isNotNullUnclean);

            Should.exist(isNotNullClean);
            isNotNullClean.should.eql("IS NOT NULL");
        });
    });

});

