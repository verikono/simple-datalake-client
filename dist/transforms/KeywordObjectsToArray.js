"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.keywordObjectsToArray = exports.TxKeywordObjectsToArray = void 0;
const stream_1 = require("stream");
/**
 * Parse a CSV stream, consequent transforms being called for each row.
 */
class TxKeywordObjectsToArray extends stream_1.Transform {
    constructor(options) {
        super();
        this.stack = [];
        this.spoolSize = options.spoolsize || 100;
    }
    _transform(chunk, encoding, callback) {
        try {
            let obj;
            try {
                obj = JSON.parse(chunk.toString());
            }
            catch (err) {
                throw new Error(`Failed parsing an inbound chunk >> ${chunk.toString()} <<`);
            }
            this.stack.push(obj);
            if (this.stack.length === this.spoolSize) {
                this.push(JSON.stringify(this.stack));
                this.stack = [];
            }
            callback();
        }
        catch (err) {
            callback(new Error(`TxSpool has failed - ${err.message}`));
        }
    }
}
exports.TxKeywordObjectsToArray = TxKeywordObjectsToArray;
const keywordObjectsToArray = (options = {}) => new TxKeywordObjectsToArray(options);
exports.keywordObjectsToArray = keywordObjectsToArray;
//# sourceMappingURL=KeywordObjectsToArray.js.map