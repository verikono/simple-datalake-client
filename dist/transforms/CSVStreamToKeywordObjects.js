"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CSVStreamToKeywordObjects = exports.TxCSVStreamToKeywordObjects = void 0;
const stream_1 = require("stream");
const Papa = __importStar(require("papaparse"));
const util_1 = require("./util");
/**
 * Parse a CSV stream, consequent transforms being called for each row.
 *
 * Limitations : the first row in the CSV file MUST be the column names.
 *
 */
class TxCSVStreamToKeywordObjects extends stream_1.Transform {
    constructor(options) {
        super();
        this.firstChunk = true;
        this.delimiterCount = 0;
        try {
            this.options = options;
            if (options.report) {
                if (!util_1.isEmptyObject(options.report))
                    throw new Error(`constructor argument "report" should be an empty object for this transformer to populate with processing information`);
                this.report = options.report;
            }
            if (options.onFirstChunk)
                this.onFirstChunk = options.onFirstChunk;
        }
        catch (err) {
            throw new Error(`TxCSVStreamToKeywordObjects has failed to construct - ${err.message}`);
        }
    }
    _transform(chunk, encoding, callback) {
        try {
            const lines = chunk.toString().split(/\r?\n/);
            if (this.firstChunk) {
                this.headings = lines[0];
                const parse = Papa.parse(this.headings);
                this.delimiter = parse.meta.delimiter;
                this.lineSplit = parse.meta.linebreak;
                this.delimiterCount = parse.data[0].length;
                this.firstChunk = false;
                if (this.report)
                    this.report.parse = parse.meta;
                if (this.onFirstChunk)
                    this.onFirstChunk(parse.meta);
            }
            else {
                lines[0] = this.lastPartial + lines[0];
                lines.unshift(this.headings);
            }
            const endline = lines[lines.length - 1];
            const endIsPartial = !(endline.split(this.delimiter).length === this.delimiterCount && endline.substr(-2) === "\n");
            this.lastPartial = endIsPartial ? lines.pop() : '';
            const parse = Papa.parse(lines.join(this.lineSplit), { header: true });
            if (this.options.dispatchRows) {
                parse.data.forEach(row => {
                    this.push(JSON.stringify(row));
                });
            }
            else {
                this.push(JSON.stringify(parse.data));
            }
            callback();
        }
        catch (err) {
            callback(new Error(`CSVStreamToKeywordObjects has failed - ${err.message}`));
        }
    }
}
exports.TxCSVStreamToKeywordObjects = TxCSVStreamToKeywordObjects;
const CSVStreamToKeywordObjects = (options = {}) => new TxCSVStreamToKeywordObjects(options);
exports.CSVStreamToKeywordObjects = CSVStreamToKeywordObjects;
//# sourceMappingURL=CSVStreamToKeywordObjects.js.map