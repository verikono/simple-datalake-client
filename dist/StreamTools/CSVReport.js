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
exports.csvReport = void 0;
const stream_1 = require("stream");
function csvReport(options = {}) {
    return __awaiter(this, void 0, void 0, function* () {
        class WrCSVReport extends stream_1.Writable {
            constructor() {
                super(...arguments);
                this.lineCount = 0;
            }
            _write(chunk, encoding, callback) {
                try {
                    const lines = chunk.toString().split(/\r?\n/);
                    this.lineCount += lines.length;
                    callback();
                }
                catch (err) {
                    callback(`WrCSVReport has failed - ${err.message}`);
                }
            }
            _final(callback) {
                console.log('CSV Report');
                console.log(`lines:- ${this.lineCount}`);
                callback();
            }
        }
        const instance = new WrCSVReport(options = {});
        return instance;
    });
}
exports.csvReport = csvReport;
//# sourceMappingURL=CSVReport.js.map