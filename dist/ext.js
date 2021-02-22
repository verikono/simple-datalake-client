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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AzureDatalakeExt = void 0;
const parse_1 = require("@fast-csv/parse");
const stream_1 = require("stream");
const zlib = __importStar(require("zlib"));
class AzureDatalakeExt {
    constructor(props) {
        this.client = null;
        const { client } = props;
        this.client = client;
    }
    /**
     *
     * @param props the property object
     * @param props.url the url of the data to perform this reduce upon.
     * @param props.reducer Function called on each row
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    reduce(props, parserOptions = {}) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            const { url, reducer } = props;
            let { accumulator } = props, i = 0, keys;
            parserOptions['key_values'] = parserOptions['key_values'] === undefined
                ? true : parserOptions['key_values'];
            let stream;
            try {
                stream = yield this.client.readableStream({ url });
                if (url.substr(-2) === 'gz')
                    stream = stream.pipe(zlib.createGunzip());
            }
            catch (err) {
                return reject(err);
            }
            stream_1.pipeline(stream, parse_1.parse(parserOptions)
                .on('data', data => {
                if (parserOptions['key_values']) {
                    if (i === 0 && !keys) {
                        keys = data;
                        return;
                    }
                    const keyed_data = keys.reduce((acc, key, i) => {
                        acc[key] = data[i];
                        return acc;
                    }, {});
                    accumulator = reducer(accumulator, keyed_data, i);
                }
                else {
                    accumulator = reducer(accumulator, data, i);
                }
            })
                .on('error', err => reject)
                .on('end', () => resolve(accumulator)), err => reject);
            return accumulator;
        }));
    }
    /**
     *
     * @param props the property object
     * @param props.eachRow Function called on each row
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    map(props, parserOptions = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                const { url } = props;
                let { mapper } = props, i = 0, promises = [], keys;
                parserOptions['key_values'] = parserOptions['key_values'] === undefined
                    ? true : parserOptions['key_values'];
                let stream;
                try {
                    stream = yield this.client.readableStream({ url });
                    if (url.substr(-2) === 'gz')
                        stream = stream.pipe(zlib.createGunzip());
                }
                catch (err) {
                    return reject(err);
                }
                try {
                    stream_1.pipeline(stream, parse_1.parse(parserOptions)
                        .on('data', data => {
                        if (parserOptions['key_values']) {
                            if (i === 0 && !keys) {
                                keys = data;
                                return;
                            }
                            const keyed_data = keys.reduce((acc, key, i) => {
                                acc[key] = data[i];
                                return acc;
                            }, {});
                            promises.push(mapper(keyed_data, i));
                        }
                        else {
                            promises.push(mapper(data, i));
                            i++;
                        }
                    })
                        .on('error', reject)
                        .on('end', () => __awaiter(this, void 0, void 0, function* () {
                        const result = yield Promise.all(promises);
                        resolve(result);
                    })), err => reject);
                }
                catch (err) {
                    return reject(err);
                }
            }));
        });
    }
    /**
     * Iterate a CSV datafile, optionally blocking I/O (default true)
     *
     * @param props the argument object
     * @param props.url String - the url of the file
     * @param props.fn Function - the function to call for each row
     * @param props.block Boolean - wait until all rows have finished, default true
     * @param parserOptions -see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    forEach(props, parserOptions = {}) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            const { url, fn } = props;
            let { block } = props, i = 0, promises = [], keys;
            parserOptions['key_values'] = parserOptions['key_values'] === undefined
                ? true : parserOptions['key_values'];
            block = block === undefined ? true : block;
            const finalize = () => resolve();
            let stream;
            try {
                stream = yield this.client.readableStream({ url });
                if (url.substr(-2) === 'gz')
                    stream = stream.pipe(zlib.createGunzip());
            }
            catch (err) {
                return reject(err);
            }
            try {
                stream_1.pipeline(stream, parse_1.parse(parserOptions)
                    .on('data', data => {
                    if (parserOptions['key_values']) {
                        if (i === 0 && !keys) {
                            keys = data;
                            return;
                        }
                        const keyed_data = keys.reduce((acc, key, i) => {
                            acc[key] = data[i];
                            return acc;
                        }, {});
                        promises.push(fn(keyed_data, i));
                    }
                    else {
                        promises.push(fn(data, i));
                        i++;
                    }
                })
                    .on('error', reject)
                    .on('end', () => __awaiter(this, void 0, void 0, function* () {
                    if (block) {
                        yield Promise.all(promises);
                        finalize();
                    }
                })), err => reject);
            }
            catch (err) {
                return reject(err);
            }
            if (!block)
                finalize();
        }));
    }
    /**
     * Get the number of rows in this datafile
     * @param props
     */
    count(props, parserOptions = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                const { url } = props;
                let stream, i = 0;
                try {
                    stream = yield this.client.readableStream({ url });
                    if (url.substr(-2) === 'gz')
                        stream = stream.pipe(zlib.createGunzip());
                }
                catch (err) {
                    return reject(err);
                }
                try {
                    stream_1.pipeline(stream, parse_1.parse(parserOptions)
                        .on('data', data => {
                        i++;
                    })
                        .on('error', err => {
                        reject(err);
                    })
                        .on('end', () => {
                        resolve(i);
                    }), err => {
                        if (err)
                            return reject(err);
                    });
                }
                catch (err) {
                    console.log('>>>');
                }
            }));
        });
    }
}
exports.AzureDatalakeExt = AzureDatalakeExt;
//# sourceMappingURL=ext.js.map