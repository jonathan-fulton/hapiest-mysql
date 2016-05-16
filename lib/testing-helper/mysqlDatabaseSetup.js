'use strict';

class MysqlDatabaseSetup {

    /**
     * @param {MysqlDatabaseSetupDao} dao
     */
    constructor(dao) {
        this._dao = dao;
    }

    /**
     *
     */
    replicateAllTables(done) {
        this._dao.replicateAllTables(done);
    }

    /**
     * @param {string[]} tables - list of tables to recreate in our unit testing database
     */
    replicateTables(tables) {
        tables.forEach(table => {
            const query = `SHOW CREATE TABLE ${this._schemaSourceDatabase}.${table};`;
            this._mysqlService.executeGenericQuery(query)
                .then(createTable => console.log(createTable));
        });
    }

}

module.exports = MysqlDatabaseSetup;