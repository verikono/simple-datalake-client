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
exports.isEmptyObject = exports.chunkType = void 0;
const Papa = __importStar(require("papaparse"));
/**
 * determine what the chunk is; it could be a stream, a keywordObject Array.
 * @param chunk
 * @returns
 */
function chunkType(chunk) {
    try {
        if (!(chunk instanceof Buffer))
            throw new Error(`argued chunk should be a buffer`);
        const str = chunk.toString();
        //determine if its JSON
        if (str[0] === '[' || str[0] === '{') {
            try {
                JSON.parse(str);
                return 'JSON';
            }
            catch (err) {
                //do nothing, its not JSON.
            }
        }
        //is it CSV
        const lines = chunk.toString().split(/\r?\n/);
        try {
            const parse = Papa.parse(lines[0]);
            if (parse.meta.delimiter)
                return 'CSV';
        }
        catch (err) {
        }
        throw new Error(`failed to determine stream type - not CSV nor JSON`);
    }
    catch (err) {
        throw new Error(`AzureDatalakeClient::util.chunkType has failed - ${err.message}`);
    }
}
exports.chunkType = chunkType;
function isEmptyObject(obj) {
    try {
        return Object.keys(obj).length === 0;
    }
    catch (err) {
        return false;
    }
}
exports.isEmptyObject = isEmptyObject;
//# sourceMappingURL=util.js.map