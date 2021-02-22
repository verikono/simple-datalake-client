"use strict";
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
const transforms_1 = require("./transforms");
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
            let { accumulator } = props, i = 0;
            let stream;
            try {
                stream = yield this.client.readableStream({ url });
            }
            catch (err) {
                return reject(err);
            }
            stream_1.pipeline(stream, transforms_1.unzipIfZipped(), parse_1.parse(parserOptions)
                .on('data', data => {
                accumulator = reducer(accumulator, data, i);
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
                let { mapper } = props, i = 0;
                let stream;
                try {
                    stream = yield this.client.readableStream({ url });
                }
                catch (err) {
                    return reject(err);
                }
                let promises = [];
                try {
                    stream_1.pipeline(stream, transforms_1.unzipIfZipped(), parse_1.parse(parserOptions)
                        .on('data', data => {
                        promises.push(mapper(data, i));
                        i++;
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
            let { block } = props, i = 0, promises = [];
            block = block === undefined ? true : block;
            const finalize = () => resolve();
            let stream;
            try {
                stream = yield this.client.readableStream({ url });
            }
            catch (err) {
                return reject(err);
            }
            try {
                stream_1.pipeline(stream, transforms_1.unzipIfZipped(), parse_1.parse(parserOptions)
                    .on('data', data => {
                    promises.push(fn(data, i));
                    i++;
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
                }
                catch (err) {
                    return reject(err);
                }
                parse_1.parseStream(stream, parserOptions)
                    .on('data', data => i++)
                    .on('error', reject)
                    .on('end', () => resolve(i));
            }));
        });
    }
}
exports.AzureDatalakeExt = AzureDatalakeExt;
//# sourceMappingURL=ext.js.map