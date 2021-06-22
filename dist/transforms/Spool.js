"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.spool = exports.TxSpool = void 0;
const stream_1 = require("stream");
/**
 * Parse a CSV stream, consequent transforms being called for each row.
 */
class TxSpool extends stream_1.Transform {
    constructor(options) {
        super();
        this.stack = [];
        this.spoolSize = 100;
        this.spoolSize = options.spoolsize;
    }
    _transform(chunk, encoding, callback) {
        try {
            this.stack.push(chunk);
            if (this.stack.length === this.spoolSize) {
                this.push(JSON.stringify(this.spoolSize));
                this.stack = [];
            }
        }
        catch (err) {
            callback(new Error(`TxSpool has failed - ${err.message}`));
        }
    }
}
exports.TxSpool = TxSpool;
const spool = (options) => new TxSpool(options);
exports.spool = spool;
//# sourceMappingURL=Spool.js.map