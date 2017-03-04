'use strict';

const Should = require('should');
const _ = require('lodash');
const VO = require('hapiest-vo');
const Sinon = require('sinon');
const Faker = require('faker');

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

class TopSecretInfo extends VO {
    constructor(args) {
        super();
        this._addProperties(args);
    }
    get id() { return this.get('id'); }
    get secretAgentCode() { return this.get('secretAgentCode'); }
    get handlerCode() { return this.get('handlerCode'); }
    get intel() { return this.get('intel'); }
    get dateAdded() { return this.get('dateAdded'); }
    get dateUpdated() { return this.get('dateUpdated'); }
    get fakePassportNumber() { return this.get('fakePassportNumber'); }
    get bankAccount() { return this.get('bankAccount'); }
    get codeName() { return this.get('codeName'); }
}

const createFromDbRow = (factory) => {
    return function(dbRow) {
        const args = {};
        Object.keys(dbRow).forEach(columnName => {
            const camelCaseColumn = _.camelCase(columnName);
            args[camelCaseColumn] = dbRow[columnName];
        });
        return new factory(args);
    }
};

const createUserFromDbRow = createFromDbRow(User);
const createTopSecretInfoFromDbRow = createFromDbRow(TopSecretInfo);

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

class TopSecretInfoDao extends MysqlDao {
    get tableName() {return 'top_secret_info';}
}
const topSecretInfoDao = new TopSecretInfoDao(MysqlDaoArgsFactory.createFromJsObj({
    mysqlService: mysqlService,
    createVoFromDbRowFunction: createTopSecretInfoFromDbRow,
    logger: logger
}));

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
        `,
        'DROP TABLE IF EXISTS top_secret_info',
        `
        CREATE TABLE top_secret_info (
          id int(11) NOT NULL AUTO_INCREMENT,
          secret_agent_code int(11) NOT NULL ,
          handler_code int(11) DEFAULT NULL,
          fake_passport_number int(11) DEFAULT NULL,
          bank_account tinyint(4) DEFAULT NULL,
          code_name varchar(255) DEFAULT NULL,
          intel varchar(255) DEFAULT NULL,
          date_added datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          date_updated datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_top_secret_info (secret_agent_code,handler_code,fake_passport_number,bank_account)
        )
        `
    ];

    mysqlService.executeQueries(queries)
        .then(() => done(), (err) => done(err));
}

function databaseTeardown(done) {
    const queries = ['DROP TABLE IF EXISTS users', 'DROP TABLE IF EXISTS top_secret_info'];
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

    describe('upsert', function() {
        beforeEach(databaseSetup);

        const insertArgs = {
            secret_agent_code: 1, handler_code: 1, fake_passport_number: 1, bank_account: 1,
            code_name: 'Duchess', intel: 'The Briefcase'
        };
        const onDupUpdateArgs = {intel: 'The Briefcase'};

        it('Should create a single row in the top_secret_info table', function() {
            return topSecretInfoDao.upsert(insertArgs, onDupUpdateArgs)
                .then(result => {
                    Should.exist(result);
                    result.should.eql(1);
                });
        });

        it('Should update a single row in the top_secret_info table', function() {
            return topSecretInfoDao.upsert(insertArgs, onDupUpdateArgs)
                .then(result => {
                    let promise = topSecretInfoDao.upsert(insertArgs,  {intel: 'Safehouse Location'});
                    return Promise.all([promise]).then(results => results[0]);
                })
                .then(result => {
                    Should.exist(result);
                    result.should.eql(2);
                });
        });

        it('Should update a single row in the top_secret_info table with a raw input', function() {
            return topSecretInfoDao.upsert(insertArgs, onDupUpdateArgs)
            .then(result => {
                let promise = topSecretInfoDao.upsert(insertArgs,  {intel: { raw: 'TRIM("     Safehouse Location   ")' }});
                return Promise.all([promise]).then(results => results[0]);
            })
            .then(result => {
                Should.exist(result);
                result.should.eql(2);
            });
        });
    });

    describe('upsertBulk', function() {
        beforeEach(databaseSetup);

        it('Should create multiple rows in the top_secret_info table', function() {
            const insertArgs = [
                {
                    secret_agent_code: 1, handler_code: 1, fake_passport_number: 1, bank_account: 1,
                    code_name: 'Duchess', intel: 'The Briefcase'
                },
                {
                    secret_agent_code: 2, handler_code: 2, fake_passport_number: 2, bank_account: 2,
                    code_name: 'Otter', intel: 'Dead Drop'
                },
                {
                    secret_agent_code: 3, handler_code: 3, fake_passport_number: 3, bank_account: 3,
                    code_name: 'Pinto', intel: 'Poison Pill'
                }
            ];
            const onDupUpdateArgs = ['intel'];

            return topSecretInfoDao.upsertBulk(insertArgs, onDupUpdateArgs)
                .then(result => {
                    Should.exist(result);
                    result.should.eql(3);
                });
        });

        it('Should update a row with a value not specified in the insert', function() {
            const insertArgs = [
                {
                    secret_agent_code: 1, handler_code: 1, fake_passport_number: 1, bank_account: 1,
                    code_name: 'Duchess', intel: 'The Briefcase'
                }
            ];

            const onDupUpdateArgs = ['intel'];

            return topSecretInfoDao.upsertBulk(insertArgs, onDupUpdateArgs)
                .then(result => {
                    let promise = topSecretInfoDao.upsertBulk(insertArgs,  { intel: 'Troop Movements' });
                    return Promise.all([promise]).then(results => results[0]);
                })
                .then(result => {
                    Should.exist(result);
                    result.should.eql(2);

                    const assertPromise = topSecretInfoDao.getOne({secretAgentCode: 1})
                        .then(info => {
                            Should.exist(info);
                            info.intel.should.eql('Troop Movements');
                        });
                    return Promise.all([assertPromise]);
                });
        });

        it('Should update multiple rows in the top_secret_info table', function() {
            const insertArgs = [
                {
                    secret_agent_code: 1, handler_code: 1, fake_passport_number: 1, bank_account: 1,
                    code_name: 'Duchess', intel: 'The Briefcase'
                },
                {
                    secret_agent_code: 2, handler_code: 2, fake_passport_number: 2, bank_account: 2,
                    code_name: 'Otter', intel: 'Dead Drop'
                }
                , {
                    secret_agent_code: 3, handler_code: 3, fake_passport_number: 3, bank_account: 3,
                    code_name: 'Pinto', intel: 'Poison Pill'
                }
            ];
            const onDupUpdateArgs = ['intel'];

            return topSecretInfoDao.upsertBulk(insertArgs, onDupUpdateArgs)
                .then(result => {
                    Should.exist(result);
                    result.should.eql(3);
                    _.set(insertArgs[0], 'intel', 'Spy Photos');
                    _.set(insertArgs[1], 'intel', 'Blackmail Material');
                    let promise = topSecretInfoDao.upsertBulk(insertArgs,  ['intel']);
                    return Promise.all([promise]).then(results => results[0]);
                })
                .then(result => {
                    Should.exist(result);
                    result.should.eql(5);
                    /*
                     * Why 5?
                     *
                     * https://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html
                     *
                     * With ON DUPLICATE KEY UPDATE, the affected-rows value per row is 1 if the row is inserted as
                     * a new row, 2 if an existing row is updated, and 0 if an existing row is set to its current
                     * values. If you specify the CLIENT_FOUND_ROWS flag to mysql_real_connect() when connecting to
                     * mysqld, the affected-rows value is 1 (not 0) if an existing row is set to its current values.
                     */
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

        it('Should return limit to 2 of 5 users', function() {
            const user1 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user2 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user3 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user4 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user5 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const userData = [user1, user2, user3, user4, user5];

            return userDao.createBulk(userData)
            .then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(5);
                const checkRowPromise1 = userDao.getAll({}, { limit: 2 })
                .then(users => {
                    Should.exist(users);
                    users.should.be.an.Array();
                    users.length.should.eql(2);

                    users[0].should.be.an.instanceOf(User);
                    users[0].firstName.should.eql(user1.firstName);
                    users[0].lastName.should.eql(user1.lastName);
                    users[0].email.should.eql(user1.email);

                    users[1].should.be.an.instanceOf(User);
                    users[1].firstName.should.eql(user2.firstName);
                    users[1].lastName.should.eql(user2.lastName);
                    users[1].email.should.eql(user2.email);
                });

                return Promise.all([checkRowPromise1]);
            });
        });

        it('Should return users in descending order', function() {
            const user1 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user2 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user3 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user4 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user5 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const userData = [user1, user2, user3, user4, user5].sort(function (a, b) {
                if (a.email > b.email) return -1;
                else if (a.email < b.email) return 1;
                return 0
            });

            return userDao.createBulk(userData)
            .then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(5);
                const checkRowPromise1 = userDao.getAll({}, { sort: { email: 'DESC', lastName: 'DESC' }})
                .then(users => {
                    Should.exist(users);
                    users.should.be.an.Array();
                    users.length.should.eql(5);

                    users[0].should.be.an.instanceOf(User);
                    users[0].firstName.should.eql(userData[0].firstName);
                    users[0].lastName.should.eql(userData[0].lastName);
                    users[0].email.should.eql(userData[0].email);

                    users[1].should.be.an.instanceOf(User);
                    users[1].firstName.should.eql(userData[1].firstName);
                    users[1].lastName.should.eql(userData[1].lastName);
                    users[1].email.should.eql(userData[1].email);

                    users[2].should.be.an.instanceOf(User);
                    users[2].firstName.should.eql(userData[2].firstName);
                    users[2].lastName.should.eql(userData[2].lastName);
                    users[2].email.should.eql(userData[2].email);

                    users[3].should.be.an.instanceOf(User);
                    users[3].firstName.should.eql(userData[3].firstName);
                    users[3].lastName.should.eql(userData[3].lastName);
                    users[3].email.should.eql(userData[3].email);

                    users[4].should.be.an.instanceOf(User);
                    users[4].firstName.should.eql(userData[4].firstName);
                    users[4].lastName.should.eql(userData[4].lastName);
                    users[4].email.should.eql(userData[4].email);
                });

                return Promise.all([checkRowPromise1]);
            });
        });

        it('Should return the last 2 of 5 users', function() {
            const user1 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user2 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user3 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user4 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user5 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const userData = [user1, user2, user3, user4, user5];

            return userDao.createBulk(userData)
            .then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(5);
                const checkRowPromise1 = userDao.getAll({}, { offset: 3, limit: 2 })
                .then(users => {
                    Should.exist(users);
                    users.should.be.an.Array();
                    users.length.should.eql(2);

                    users[0].should.be.an.instanceOf(User);
                    users[0].firstName.should.eql(user4.firstName);
                    users[0].lastName.should.eql(user4.lastName);
                    users[0].email.should.eql(user4.email);

                    users[1].should.be.an.instanceOf(User);
                    users[1].firstName.should.eql(user5.firstName);
                    users[1].lastName.should.eql(user5.lastName);
                    users[1].email.should.eql(user5.email);
                });

                return Promise.all([checkRowPromise1]);
            });
        });
    });

    describe('getAllAndCount', function() {
        beforeEach(databaseSetup);

        it('Should return two users and the count', function() {
            return userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ])
            .then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(2);
                const checkRowPromise1 = userDao.getAllAndCount({lastName: 'Doe'})
                .then(result => {
                    const count = result.count;
                    Should.exist(count);
                    count.should.eql(2);

                    const users = result.results;
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

        it('Should return limit to 2 of 5 users and the count', function() {
            const user1 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user2 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user3 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user4 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user5 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const userData = [user1, user2, user3, user4, user5];

            return userDao.createBulk(userData)
            .then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(5);
                const checkRowPromise1 = userDao.getAllAndCount({}, { limit: 2 })
                .then(result => {
                    const count = result.count;
                    Should.exist(count);
                    count.should.eql(5);

                    const users = result.results;
                    Should.exist(users);
                    users.should.be.an.Array();
                    users.length.should.eql(2);

                    users[0].should.be.an.instanceOf(User);
                    users[0].firstName.should.eql(user1.firstName);
                    users[0].lastName.should.eql(user1.lastName);
                    users[0].email.should.eql(user1.email);

                    users[1].should.be.an.instanceOf(User);
                    users[1].firstName.should.eql(user2.firstName);
                    users[1].lastName.should.eql(user2.lastName);
                    users[1].email.should.eql(user2.email);
                });

                return Promise.all([checkRowPromise1]);
            });
        });

        it('Should return users in descending order and the count', function() {
            const user1 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user2 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user3 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user4 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user5 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const userData = [user1, user2, user3, user4, user5].sort(function (a, b) {
                if (a.email > b.email) return -1;
                else if (a.email < b.email) return 1;
                return 0
            });

            return userDao.createBulk(userData)
            .then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(5);
                const checkRowPromise1 = userDao.getAllAndCount({}, { sort: { email: 'DESC', lastName: 'DESC' }})
                .then(result => {
                    const count = result.count;
                    Should.exist(count);
                    count.should.eql(5);

                    const users = result.results;
                    Should.exist(users);
                    users.should.be.an.Array();
                    users.length.should.eql(5);

                    users[0].should.be.an.instanceOf(User);
                    users[0].firstName.should.eql(userData[0].firstName);
                    users[0].lastName.should.eql(userData[0].lastName);
                    users[0].email.should.eql(userData[0].email);

                    users[1].should.be.an.instanceOf(User);
                    users[1].firstName.should.eql(userData[1].firstName);
                    users[1].lastName.should.eql(userData[1].lastName);
                    users[1].email.should.eql(userData[1].email);

                    users[2].should.be.an.instanceOf(User);
                    users[2].firstName.should.eql(userData[2].firstName);
                    users[2].lastName.should.eql(userData[2].lastName);
                    users[2].email.should.eql(userData[2].email);

                    users[3].should.be.an.instanceOf(User);
                    users[3].firstName.should.eql(userData[3].firstName);
                    users[3].lastName.should.eql(userData[3].lastName);
                    users[3].email.should.eql(userData[3].email);

                    users[4].should.be.an.instanceOf(User);
                    users[4].firstName.should.eql(userData[4].firstName);
                    users[4].lastName.should.eql(userData[4].lastName);
                    users[4].email.should.eql(userData[4].email);
                });

                return Promise.all([checkRowPromise1]);
            });
        });

        it('Should return the last 2 of 5 users and the count', function() {
            const user1 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user2 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user3 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user4 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user5 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const userData = [user1, user2, user3, user4, user5];

            return userDao.createBulk(userData)
            .then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(5);
                const checkRowPromise1 = userDao.getAllAndCount({}, { offset: 3, limit: 2 })
                .then(result => {
                    const count = result.count;
                    Should.exist(count
                    );
                    count.should.eql(5);

                    const users = result.results;
                    Should.exist(users);
                    users.should.be.an.Array();
                    users.length.should.eql(2);

                    users[0].should.be.an.instanceOf(User);
                    users[0].firstName.should.eql(user4.firstName);
                    users[0].lastName.should.eql(user4.lastName);
                    users[0].email.should.eql(user4.email);

                    users[1].should.be.an.instanceOf(User);
                    users[1].firstName.should.eql(user5.firstName);
                    users[1].lastName.should.eql(user5.lastName);
                    users[1].email.should.eql(user5.email);
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

    describe('stream', function() {
        beforeEach(databaseSetup);

        it('Should stream two users', function(done) {

            userDao.createBulk([
                    {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                    {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
                ])
                .then((numRows) => {
                    Should.exist(numRows);
                    numRows.should.eql(2);

                    const users = [];
                    userDao.stream({lastName: 'Doe'})
                        .on('data', dbRow => users.push(dbRow))
                        .on('end', () => {
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

                            done();
                        });

                });
        });

        it('Should stream limit to 2 of 5 users', function(done) {
            const user1 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user2 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user3 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user4 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user5 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const userData = [user1, user2, user3, user4, user5];

            userDao.createBulk(userData).then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(5);
                const users = [];
                userDao.stream({}, { limit: 2 })
                    .on('data', dbRow => users.push(dbRow))
                    .on('end', () => {
                        Should.exist(users);
                        users.should.be.an.Array();
                        users.length.should.eql(2);

                        users[0].should.be.an.instanceOf(User);
                        users[0].firstName.should.eql(user1.firstName);
                        users[0].lastName.should.eql(user1.lastName);
                        users[0].email.should.eql(user1.email);

                        users[1].should.be.an.instanceOf(User);
                        users[1].firstName.should.eql(user2.firstName);
                        users[1].lastName.should.eql(user2.lastName);
                        users[1].email.should.eql(user2.email);

                        done();
                    });
            });
        });

        it('Should stream users in descending order', function(done) {
            const user1 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user2 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user3 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user4 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user5 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const userData = [user1, user2, user3, user4, user5].sort(function (a, b) {
                if (a.email > b.email) return -1;
                else if (a.email < b.email) return 1;
                return 0
            });

            userDao.createBulk(userData).then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(5);
                const users = [];
                userDao.stream({}, { sort: { email: 'DESC', lastName: 'DESC' }})
                    .on('data', dbRow => users.push(dbRow))
                    .on('end', () => {
                        Should.exist(users);
                        users.should.be.an.Array();
                        users.length.should.eql(5);

                        users[0].should.be.an.instanceOf(User);
                        users[0].firstName.should.eql(userData[0].firstName);
                        users[0].lastName.should.eql(userData[0].lastName);
                        users[0].email.should.eql(userData[0].email);

                        users[1].should.be.an.instanceOf(User);
                        users[1].firstName.should.eql(userData[1].firstName);
                        users[1].lastName.should.eql(userData[1].lastName);
                        users[1].email.should.eql(userData[1].email);

                        users[2].should.be.an.instanceOf(User);
                        users[2].firstName.should.eql(userData[2].firstName);
                        users[2].lastName.should.eql(userData[2].lastName);
                        users[2].email.should.eql(userData[2].email);

                        users[3].should.be.an.instanceOf(User);
                        users[3].firstName.should.eql(userData[3].firstName);
                        users[3].lastName.should.eql(userData[3].lastName);
                        users[3].email.should.eql(userData[3].email);

                        users[4].should.be.an.instanceOf(User);
                        users[4].firstName.should.eql(userData[4].firstName);
                        users[4].lastName.should.eql(userData[4].lastName);
                        users[4].email.should.eql(userData[4].email);

                        done();
                    });
            });
        });

        it('Should stream the last 2 of 5 users', function(done) {
            const user1 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user2 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user3 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user4 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const user5 = {firstName: Faker.name.firstName(), lastName: Faker.name.lastName(), email: Faker.internet.email()};
            const userData = [user1, user2, user3, user4, user5];

            userDao.createBulk(userData).then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(5);
                const users = [];
                userDao.stream({}, { offset: 3, limit: 2 })
                  .on('data', dbRow => users.push(dbRow))
                  .on('end', () => {
                      Should.exist(users);
                      users.should.be.an.Array();
                      users.length.should.eql(2);

                      users[0].should.be.an.instanceOf(User);
                      users[0].firstName.should.eql(user4.firstName);
                      users[0].lastName.should.eql(user4.lastName);
                      users[0].email.should.eql(user4.email);

                      users[1].should.be.an.instanceOf(User);
                      users[1].firstName.should.eql(user5.firstName);
                      users[1].lastName.should.eql(user5.lastName);
                      users[1].email.should.eql(user5.email);

                      done();
                    });
            });
        });
    });

    describe('streamFromMaster', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.streamQueryFromMaster.restore) {
                userDao._mysqlService.streamQueryFromMaster.restore();
            }
        });

        it('Should stream two users', function(done) {
            const streamFromMasterSpy = Sinon.spy(userDao._mysqlService, 'streamQueryFromMaster');
            userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ]).then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(2);
                const users = [];
                userDao.streamFromMaster({lastName: 'Doe'})
                    .on('data', dbRow => users.push(dbRow))
                    .on('end', () => {
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

                        streamFromMasterSpy.calledOnce.should.be.True();

                        done();
                    });
            });
        });
    });

    describe('streamFromSql', function() {
        beforeEach(databaseSetup);

        it('Should stream two rows with only first_name populated', function(done) {
            userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ]).then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(2);
                const sql = "SELECT first_name FROM users WHERE last_name='Doe'";
                const dbRows = [];
                userDao.streamFromSql(sql)
                    .on('data', dbRow => dbRows.push(dbRow))
                    .on('end', () => {
                        Should.exist(dbRows);
                        dbRows.should.be.an.Array();
                        dbRows.length.should.eql(2);

                        dbRows[0].firstName.should.eql('John');
                        dbRows[1].firstName.should.eql('Jane');

                        Should.not.exist(dbRows[0].email);
                        Should.not.exist(dbRows[0].lastName);
                        Should.not.exist(dbRows[1].email);
                        Should.not.exist(dbRows[1].lastName);

                        done();
                    });
            });
        });

        it('It should pass errors through the stream', function(done) {
          let errorEmitted = false;
          let dataEmitted = false;
          userDao.streamFromSql('SELECT ERROR SYNTAX...')
            .on('data', () => dataEmitted = true)
            .on('error', (error) => {
                errorEmitted = true;
                error.should.have.property('code');
                error.code.should.eql('ER_PARSE_ERROR');
            })
            .on('end', () => {
                errorEmitted.should.be.true;
                dataEmitted.should.be.false;
                done();
            });
        });
    });

    describe('streamFromSqlFromMaster', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.streamQueryFromMaster.restore) {
                userDao._mysqlService.streamQueryFromMaster.restore();
            }
        });

        it('Should stream two rows with only first_name populated', function(done) {
            const streamFromMasterSpy = Sinon.spy(userDao._mysqlService, 'streamQueryFromMaster');
            userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ]).then((numRows) => {
                Should.exist(numRows);
                numRows.should.eql(2);
                const sql = "SELECT first_name FROM users WHERE last_name='Doe'";
                const dbRows = [];
                userDao.streamFromSqlFromMaster(sql)
                    .on('data', dbRow => dbRows.push(dbRow))
                    .on('end', () => {
                        Should.exist(dbRows);
                        dbRows.should.be.an.Array();
                        dbRows.length.should.eql(2);

                        dbRows[0].firstName.should.eql('John');
                        dbRows[1].firstName.should.eql('Jane');

                        Should.not.exist(dbRows[0].email);
                        Should.not.exist(dbRows[0].lastName);
                        Should.not.exist(dbRows[1].email);
                        Should.not.exist(dbRows[1].lastName);

                        streamFromMasterSpy.calledOnce.should.be.True();

                        done();
                    });
            });
        });

        it('It should pass errors through the stream', function(done) {
          let errorEmitted = false;
          let dataEmitted = false;
          userDao.streamFromSqlFromMaster('SELECT ERROR SYNTAX...')
            .on('data', () => dataEmitted = true)
            .on('error', (error) => {
                errorEmitted = true;
                error.should.have.property('code');
                error.code.should.eql('ER_PARSE_ERROR');
            })
            .on('end', () => {
                errorEmitted.should.be.true;
                dataEmitted.should.be.false;
                done();
            });
        });
    });

    describe('streamFromSqlRaw', function() {
        beforeEach(databaseSetup);

        it('Should stream two rows with only first_name populated', function(done) {
            userDao.createBulk([
                    {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                    {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ]).then((numRows) => {
                const sql = "SELECT first_name FROM users WHERE last_name='Doe'";
                const dbRows = [];
                userDao.streamFromSqlRaw(sql)
                    .on('data', dbRow => dbRows.push(dbRow))
                    .on('end', () => {
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

                        done();
                    });
            });
        });

        it('It should pass errors through the stream', function(done) {
          let errorEmitted = false;
          let dataEmitted = false;
          userDao.streamFromSqlRaw('SELECT ERROR SYNTAX...')
            .on('data', () => dataEmitted = true)
            .on('error', (error) => {
                errorEmitted = true;
                error.should.have.property('code');
                error.code.should.eql('ER_PARSE_ERROR');
            })
            .on('end', () => {
                errorEmitted.should.be.true;
                dataEmitted.should.be.false;
                done();
            });
        });
    });

    describe('streamFromSqlFromMasterRaw', function() {
        beforeEach(databaseSetup);
        afterEach(() => {
            if (userDao._mysqlService.streamQueryFromMaster.restore) {
                userDao._mysqlService.streamQueryFromMaster.restore();
            }
        });

        it('Should stream two rows with only first_name populated', function(done) {
            const streamFromMasterSpy = Sinon.spy(userDao._mysqlService, 'streamQueryFromMaster');
            userDao.createBulk([
                {firstName: 'John', lastName: 'Doe', email: 'john.doe@gmail.com'},
                {firstName: 'Jane', lastName: 'Doe', email: 'jane.doe@gmail.com'}
            ]).then((numRows) => {

                  const sql = "SELECT first_name FROM users WHERE last_name='Doe'";
                  const dbRows = [];
                  userDao.streamFromSqlFromMasterRaw(sql)
                      .on('data', dbRow => dbRows.push(dbRow))
                      .on('end', () => {
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

                          streamFromMasterSpy.calledOnce.should.be.True();

                          done();
                      });
              });
        });

        it('It should pass errors through the stream', function(done) {
          let errorEmitted = false;
          let dataEmitted = false;
          userDao.streamFromSqlFromMasterRaw('SELECT ERROR SYNTAX...')
            .on('data', () => dataEmitted = true)
            .on('error', (error) => {
                errorEmitted = true;
                error.should.have.property('code');
                error.code.should.eql('ER_PARSE_ERROR');
            })
            .on('end', () => {
                errorEmitted.should.be.true;
                dataEmitted.should.be.false;
                done();
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
