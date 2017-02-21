'use strict';

const Should = require('should');
const MysqlQuery = require('../../lib/mysqlQuery');

describe('MysqlQuery', function(){

    describe('validateSelect', function() {
        it('Should not throw an error for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            let error = null;
            try {
                const result = MysqlQuery.validateSelect(query);
            } catch(e) {
                error = e;
            }
            Should.not.exist(error);
        });
        it('Should throw an error for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            let error = null;
            try {
                const result = MysqlQuery.validateSelect(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid SELECT query");
        });
        it('Should throw an error for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            let error = null;
            try {
                const result = MysqlQuery.validateSelect(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid SELECT query");
        });
        it('Should throw an error for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            let error = null;
            try {
                const result = MysqlQuery.validateSelect(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid SELECT query");
        });
        it('Should throw an error for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!') ON DUPLICATE KEY UPDATE first_name = 'firstName', last_name = 'lastName', password = 'boom!'";
            let error = null;
            try {
                const result = MysqlQuery.validateSelect(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid SELECT query");
        });
        it('Should throw an error for DELETE queries', function() {
            const query = "    DELETE FROM someTable WHERE value > 10 ;";
            let error = null;
            try {
                const result = MysqlQuery.validateSelect(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid SELECT query");
        });
    });

    describe('validateInsert', function() {
        it('Should throw an error for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            let error = null;
            try {
                const result = MysqlQuery.validateInsert(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid INSERT query");
        });
        it('Should not throw an error for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            let error = null;
            try {
                const result = MysqlQuery.validateInsert(query);
            } catch(e) {
                error = e;
            }
            Should.not.exist(error);
        });
        it('Should not throw an error for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            let error = null;
            try {
                const result = MysqlQuery.validateInsert(query);
            } catch(e) {
                error = e;
            }
            Should.not.exist(error);
        });
        it('Should throw an error for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!') ON DUPLICATE KEY UPDATE first_name = 'firstName', last_name = 'lastName', password = 'boom!'";
            let error = null;
            try {
                const result = MysqlQuery.validateInsert(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid INSERT query");
        });
        it('Should throw an error for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            let error = null;
            try {
                const result = MysqlQuery.validateInsert(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid INSERT query");
        });
        it('Should throw an error for DELETE queries', function() {
            const query = "    DELETE FROM someTable WHERE value > 10 ;";
            let error = null;
            try {
                const result = MysqlQuery.validateInsert(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid INSERT query");
        });
    });

    describe('validateUpdate', function() {
        it('Should throw an error for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            let error = null;
            try {
                const result = MysqlQuery.validateUpdate(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid UPDATE query");
        });
        it('Should throw an error for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            let error = null;
            try {
                const result = MysqlQuery.validateUpdate(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid UPDATE query");
        });
        it('Should throw an error for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            let error = null;
            try {
                const result = MysqlQuery.validateUpdate(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid UPDATE query");
        });
        it('Should not throw an error for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            let error = null;
            try {
                const result = MysqlQuery.validateUpdate(query);
            } catch(e) {
                error = e;
            }
            Should.not.exist(error);
        });
        it('Should throw an error for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!') ON DUPLICATE KEY UPDATE first_name = 'firstName', last_name = 'lastName', password = 'boom!'";
            let error = null;
            try {
                const result = MysqlQuery.validateUpdate(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid UPDATE query");
        });
        it('Should throw an error for DELETE queries', function() {
            const query = "    DELETE FROM someTable WHERE value > 10 ;";
            let error = null;
            try {
                const result = MysqlQuery.validateUpdate(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid UPDATE query");
        });
    });

    describe('validateUpsert', function() {
        it('Should throw an error for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            let error = null;
            try {
                const result = MysqlQuery.validateUpsert(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid INSERT ... ON DUPLICATE KEY UPDATE query");
        });
        it('Should throw an error for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            let error = null;
            try {
                const result = MysqlQuery.validateUpsert(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid INSERT ... ON DUPLICATE KEY UPDATE query");
        });
        it('Should throw an error for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            let error = null;
            try {
                const result = MysqlQuery.validateUpsert(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid INSERT ... ON DUPLICATE KEY UPDATE query");
        });
        it('Should throw an error for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            let error = null;
            try {
                const result = MysqlQuery.validateUpsert(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid INSERT ... ON DUPLICATE KEY UPDATE query");
        });
        it('Should not throw an error for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, email) VALUES ('Cecil', 'Palmer', 'cecil@nightvalecommunityradio.com') ON DUPLICATE KEY UPDATE first_name = 'Cecil', last_name = 'Palmer', email = 'cecil@nightvalecommunityradio.com'";
            let error = null;
            try {
                const result = MysqlQuery.validateUpsert(query);
            } catch(e) {
                error = e;
            }
            Should.not.exist(error);
        });
    });

    describe('validateDelete', function() {
        it('Should throw an error for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            let error = null;
            try {
                const result = MysqlQuery.validateDelete(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid DELETE query");
        });
        it('Should throw an error for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            let error = null;
            try {
                const result = MysqlQuery.validateDelete(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid DELETE query");
        });
        it('Should throw an error for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            let error = null;
            try {
                const result = MysqlQuery.validateDelete(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid DELETE query");
        });
        it('Should throw an error for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            let error = null;
            try {
                const result = MysqlQuery.validateDelete(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid DELETE query");
        });
        it('Should throw an error for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!') ON DUPLICATE KEY UPDATE first_name = 'firstName', last_name = 'lastName', password = 'boom!'";
            let error = null;
            try {
                const result = MysqlQuery.validateDelete(query);
            } catch(e) {
                error = e;
            }
            Should.exist(error);
            error.message.should.eql("Invalid DELETE query");
        });
        it('Should not throw an error for DELETE queries', function() {
            const query = "    DELETE FROM someTable WHERE value > 10 ;";
            let error = null;
            try {
                const result = MysqlQuery.validateDelete(query);
            } catch(e) {
                error = e;
            }
            Should.not.exist(error);
        });
    });


    describe('isSelect', function() {
        it('Should return true for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            const result = MysqlQuery.isSelect(query);
            result.should.eql(true);
        });
        it('Should return false for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            const result = MysqlQuery.isSelect(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            const result = MysqlQuery.isSelect(query);
            result.should.eql(false);
        });
        it('Should return false for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            const result = MysqlQuery.isSelect(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!') ON DUPLICATE KEY UPDATE first_name = 'firstName', last_name = 'lastName', password = 'boom!'";
            const result = MysqlQuery.isSelect(query);
            result.should.eql(false);
        });
        it('Should return false for DELETE queries', function() {
            const query = "    DELETE FROM someTable WHERE value > 10 ;";
            const result = MysqlQuery.isSelect(query);
            result.should.eql(false);
        });
    });

    describe('isInsert', function() {
        it('Should return false for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            const result = MysqlQuery.isInsert(query);
            result.should.eql(false);
        });
        it('Should return true for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            const result = MysqlQuery.isInsert(query);
            result.should.eql(true);
        });
        it('Should return true for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            const result = MysqlQuery.isInsert(query);
            result.should.eql(true);
        });
        it('Should return false for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            const result = MysqlQuery.isInsert(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!') ON DUPLICATE KEY UPDATE first_name = 'firstName', last_name = 'lastName', password = 'boom!'";
            const result = MysqlQuery.isInsert(query);
            result.should.eql(false);
        });
        it('Should return false for DELETE queries', function() {
            const query = "    DELETE FROM someTable WHERE value > 10 ;";
            const result = MysqlQuery.isInsert(query);
            result.should.eql(false);
        });
    });

    describe('isUpdate', function() {
        it('Should return false for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            const result = MysqlQuery.isUpdate(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            const result = MysqlQuery.isUpdate(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            const result = MysqlQuery.isUpdate(query);
            result.should.eql(false);
        });
        it('Should return true for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            const result = MysqlQuery.isUpdate(query);
            result.should.eql(true);
        });
        it('Should return false for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!') ON DUPLICATE KEY UPDATE first_name = 'firstName', last_name = 'lastName', password = 'boom!'";
            const result = MysqlQuery.isUpdate(query);
            result.should.eql(false);
        });
        it('Should return false for DELETE queries', function() {
            const query = "    DELETE FROM someTable WHERE value > 10 ;";
            const result = MysqlQuery.isUpdate(query);
            result.should.eql(false);
        });
    });

    describe('isUpsert', function() {
        it('Should return true for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!') ON DUPLICATE KEY UPDATE first_name = 'firstName', last_name = 'lastName', password = 'boom!'";
            const result = MysqlQuery.isUpsert(query);
            result.should.eql(true);
        });
        it('Should return false for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            const result = MysqlQuery.isUpsert(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            const result = MysqlQuery.isUpsert(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            const result = MysqlQuery.isUpsert(query);
            result.should.eql(false);
        });
        it('Should return false for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            const result = MysqlQuery.isUpsert(query);
            result.should.eql(false);
        });
        it('Should return false for DELETE queries', function() {
            const query = "    DELETE FROM someTable WHERE value > 10 ;";
            const result = MysqlQuery.isUpsert(query);
            result.should.eql(false);
        });
    });

    describe('isDelete', function() {
        it('Should return false for SELECT queries', function() {
            const query = "    SELECT * FROM someTable  ; ";
            const result = MysqlQuery.isDelete(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT queries', function() {
            const query = "    INSERT INTO table(one,two) VALUES (1,2);";
            const result = MysqlQuery.isDelete(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT queries with SELECTs', function() {
            const query = "    INSERT INTO table(one,two) SELECT one, two FROM table;";
            const result = MysqlQuery.isDelete(query);
            result.should.eql(false);
        });
        it('Should return false for UPDATE queries', function() {
            const query = "    UPDATE table SET one=1 WHERE id=2 ;";
            const result = MysqlQuery.isDelete(query);
            result.should.eql(false);
        });
        it('Should return false for INSERT ... ON DUPLICATE KEY UPDATE queries', function() {
            const query = "INSERT INTO users (first_name, last_name, password) VALUES ('firstName', 'lastName', 'boom!') ON DUPLICATE KEY UPDATE first_name = 'firstName', last_name = 'lastName', password = 'boom!'";
            const result = MysqlQuery.isDelete(query);
            result.should.eql(false);
        });
        it('Should return true for DELETE queries', function() {
            const query = "    DELETE FROM someTable WHERE value > 10 ;";
            const result = MysqlQuery.isDelete(query);
            result.should.eql(true);
        });
    });

});
