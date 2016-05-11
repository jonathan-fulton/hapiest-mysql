'use strict';

const VO = require('hapiest-vo');

class MysqlModificationResult extends VO {

    /**
     * @param {object} insertResult
     */
    constructor(insertResult) {
        super();

        this._addProperties(insertResult);
    }

    /**
     * @returns {int}
     */
    get affectedRows() { return this.get('affectedRows'); }

    /**
     * @returns {int}
     */
    get insertId() { return this.get('insertId'); }

    /**
     * @returns {int} - relevant for UPDATEs
     */
    get changedRows() { return this.get('changedRows'); }
}

module.exports = MysqlModificationResult;
