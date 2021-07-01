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
exports.eachRow = exports.TxEachRow = void 0;
const stream_1 = require("stream");
const Papa = __importStar(require("papaparse"));
class TxEachRow extends stream_1.Transform {
    constructor(options) {
        super();
        this.processedFirstRow = false;
        this.rowNum = 0;
        this.promises = [];
        this.meta = null;
        this.delimiterCount = 0;
        this.options = options;
        if (!this.options.parserOptions)
            this.options.parserOptions = {};
    }
    _transform(chunk, encoding, callback) {
        try {
            const lines = chunk.toString().split(/\r?\n/);
            if (!this.processedFirstRow) {
                this.headings = lines[0];
                const parse = Papa.parse(this.headings);
                this.meta = parse.meta;
                this.delimiter = parse.meta.delimiter;
                this.linebreak = parse.meta.linebreak;
                this.delimiterCount = parse.data[0].length;
                this.processedFirstRow = true;
            }
            else {
                lines[0] = this.lastPartial + lines[0];
                lines.unshift(this.headings);
            }
            const endline = lines[lines.length - 1];
            const endIsPartial = !(endline.split(this.delimiter).length === this.delimiterCount && endline.substr(-2) === "\n");
            this.lastPartial = endIsPartial ? lines.pop() : '';
            const { data } = Papa.parse(lines.join(this.linebreak), { header: true });
            if (this.options.onRow) {
                data.forEach(data => {
                    const result = this.options.onRow(data, this.rowNum);
                    this.rowNum++;
                    if (result instanceof Promise)
                        this.promises.push(result);
                });
            }
            this.push(chunk);
            callback();
        }
        catch (err) {
            callback(new Error(`TxEachRow has failed - ${err.message}`));
        }
    }
    _final(callback) {
        new Promise((resolve, reject) => {
            if (this.promises.length) {
                Promise.all(this.promises)
                    .then(() => resolve(true))
                    .catch(err => {
                    reject(new Error(`TxEachRow received an error from one of its mappers - ${err.message}`));
                });
            }
            else {
                resolve(true);
            }
        })
            .then(() => {
            if (this.options.onEnd) {
                const endResult = this.options.onEnd(this.meta);
                if (endResult instanceof Promise) {
                    Promise.all([endResult]).then(() => {
                        callback();
                    });
                }
                else {
                    callback();
                }
            }
        });
    }
}
exports.TxEachRow = TxEachRow;
const eachRow = (options = {}) => new TxEachRow(options);
exports.eachRow = eachRow;
//# sourceMappingURL=EachRow.js.map