'use strict';

const VO = require('hapiest-vo');

// @TODO: create insert/update/delete variations and get rid of this
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
