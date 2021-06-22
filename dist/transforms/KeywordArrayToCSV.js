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
exports.keywordArrayToCSV = exports.TxKeywordArrayToCSV = void 0;
const stream_1 = require("stream");
const Papa = __importStar(require("papaparse"));
class TxKeywordArrayToCSV extends stream_1.Transform {
    constructor(options) {
        super();
        this.firstChunk = true;
        this.parseOptions = {};
        this.matches = [];
        if (options.delimiter)
            this.parseOptions['delimiter'] = options.delimiter;
    }
    _transform(chunk, encoding, callback) {
        try {
            const parseOptions = Object.assign({}, this.parseOptions, { header: this.firstChunk });
            const csv = Papa.unparse(chunk.toString(), parseOptions);
            // const p = JSON.parse(chunk.toString());
            // p.forEach(obj => {
            //     const conflict = this.matches.find(m => m === obj.promo_id)
            //     if(conflict) {
            //         console.log('#######');
            //     }
            //     this.matches.push(obj.promo_id);
            // })
            // console.log('chunk');
            this.push(csv);
            this.firstChunk = false;
            callback();
        }
        catch (err) {
            callback(new Error(`TxKeywrodArrayToCSV has failed - ${err.message}`));
        }
    }
}
exports.TxKeywordArrayToCSV = TxKeywordArrayToCSV;
const keywordArrayToCSV = (options = {}) => new TxKeywordArrayToCSV(options);
exports.keywordArrayToCSV = keywordArrayToCSV;
//# sourceMappingURL=KeywordArrayToCSV.js.map