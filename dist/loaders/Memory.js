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
exports.toGlobalMemory = void 0;
const stream_1 = require("stream");
function toGlobalMemory(options = {}) {
    return __awaiter(this, void 0, void 0, function* () {
        class WrGlobalMemory extends stream_1.Writable {
            constructor() {
                super(...arguments);
                this.callcount = 0;
                this.rowcount = 0;
            }
            _write(chunk, encoding, callback) {
                try {
                    console.log(`write ${this.callcount + 1}`);
                    this.callcount++;
                    callback();
                }
                catch (err) {
                    callback(new Error(`failure!`));
                }
            }
            _final(callback) {
                console.log('final');
                callback();
            }
        }
        const instance = new WrGlobalMemory(options = {});
        return instance;
    });
}
exports.toGlobalMemory = toGlobalMemory;
//# sourceMappingURL=Memory.js.map