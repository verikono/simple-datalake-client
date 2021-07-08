"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.applyAzureKeys = exports.TxSetAzureKeys = void 0;
const stream_1 = require("stream");
/**
 * Apply modifications across data represented in key/value objects.
 */
class TxSetAzureKeys extends stream_1.Transform {
    /**
     *
     * @param options keyword object
     * @param options.partitionkey String|String[]|Function - if a string the partitionKey is assumed to be the column name of the partitionKey in the data, an array is a compound pk and a function
     * @param options.rowkey String|String[]|Function - if a string the rowKey is assumed to be the column name of the rowkey in the data, an array is a compound pk and a function
    */
    constructor(options) {
        super();
        options = options || {};
        this.pk = options.partitionKey;
        this.rk = options.rowKey;
    }
    derivePK(keyed_row) {
        switch (typeof this.pk) {
            case 'string':
                return keyed_row[this.pk];
            case 'object':
                if (!Array.isArray(this.pk))
                    throw Error(`unable to use argued pk - expected string|string[]|Function - got ${typeof this.pk}`);
                return this.pk.reduce((acc, key) => acc.concat(keyed_row[key]), '');
            case 'function':
                try {
                    const result = this.pk(keyed_row);
                    if (!result)
                        throw new Error('partitionKey functions must return a truthy result');
                    return result;
                }
                catch (err) {
                    throw new Error(`partitionKey function thew an error - ${err.message}`);
                }
            default:
                throw new Error(`unable to use argued partitionKey - expected string | string[] | Function - got ${typeof this.pk}`);
        }
    }
    deriveRK(keyed_row) {
        switch (typeof this.rk) {
            case 'string':
                return keyed_row[this.rk];
            case 'object':
                if (!Array.isArray(this.rk))
                    throw Error(`unable to use argued pk - expected string|string[]|Function - got ${typeof this.rk}`);
                return this.rk.reduce((acc, key) => acc.concat(keyed_row[key]), '');
            case 'function':
                try {
                    const result = this.rk(keyed_row);
                    if (!result)
                        throw new Error('Rowkey functions must return a truthy result');
                    return result;
                }
                catch (err) {
                    throw new Error(`Rowkey function thew an error - ${err.message}`);
                }
            default:
                throw new Error(`unable to use argued rowKey - expected string | string[] | Function - got ${typeof this.pk}`);
        }
    }
    /**
     * Apply a modification if its in the modificaiton stack otherwise return the keyed_row unmodified.
     *
     * @param keyed_row
     * @returns
     */
    applyKeys(keyed_row) {
        const pk = this.derivePK(keyed_row);
        const rk = this.deriveRK(keyed_row);
        return Object.assign(keyed_row, { partitionKey: pk, rowKey: rk });
    }
    _transform(chunk, encoding, callback) {
        try {
            let data;
            try {
                data = JSON.parse(chunk);
            }
            catch (err) {
                throw new Error(`failed parsing a chunk from JSON - is the data keyword rows?`);
            }
            if (Array.isArray(data)) {
                const rows = data.map(row => this.applyKeys(row));
                this.push(JSON.stringify(rows));
            }
            else {
                this.push(this.applyKeys(data));
            }
            callback();
        }
        catch (err) {
            callback(new Error(`TxApplyMutations has failed - ${err.message}`));
        }
    }
    _final(callback) {
        callback();
    }
}
exports.TxSetAzureKeys = TxSetAzureKeys;
const applyAzureKeys = (options) => new TxSetAzureKeys(options);
exports.applyAzureKeys = applyAzureKeys;
//# sourceMappingURL=SetAzureKeys.js.map