"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.addColumns = exports.TxAddCSVColumns = void 0;
const stream_1 = require("stream");
const util_1 = require("./util");
/**
 * Apply modifications across data represented in key/value objects.
 */
class TxAddCSVColumns extends stream_1.Transform {
    /**
     *
     * @param options keyword object
     * @param options.columns a keyword object where the key is the new column name and value is the default value for this column
     */
    constructor(options) {
        super();
        options = options || {};
        this.columns = options.columns;
        this.streamType = null;
    }
    _transform(chunk, encoding, callback) {
        try {
            if (!this.streamType)
                this.streamType = util_1.chunkType(chunk);
            switch (this.streamType) {
                case 'JSON':
                    let parse = JSON.parse(chunk);
                    Object.keys(this.columns).forEach(key => {
                        const value = this.columns[key];
                        parse = parse.map(obj => Object.assign(obj, { [key]: value }));
                    });
                    this.push(JSON.stringify(parse));
                    break;
                default:
                    throw new Error(`Unsupported stream type ${this.streamType} - convert to JSON using CSVStreamToKeywordObjects`);
            }
            callback();
        }
        catch (err) {
            callback(new Error(`TxAddCSVColumns has failed - ${err.message}`));
        }
    }
    _final(callback) {
        callback();
    }
}
exports.TxAddCSVColumns = TxAddCSVColumns;
const addColumns = (options) => new TxAddCSVColumns(options);
exports.addColumns = addColumns;
//# sourceMappingURL=AddColumns.js.map