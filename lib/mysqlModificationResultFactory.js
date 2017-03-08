'use strict';

const MysqlInsertResult = require('./mysqlModificationResult');

class MysqlModificationResultFactory {

    /**
     * @param {OkPacket} mysqlResult
     */
    static createFromResult(mysqlResult) {

        const resultObj = {
            affectedRows: mysqlResult.affectedRows,
            insertId: mysqlResult.insertId,
            changedRows: mysqlResult.changedRows
        };

        return new MysqlInsertResult(resultObj);
    }

}

module.exports = MysqlModificationResultFactory;
