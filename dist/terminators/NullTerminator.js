"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.nullTerminator = exports.TrmNullTerminator = void 0;
const stream_1 = require("stream");
class TrmNullTerminator extends stream_1.Writable {
    _write(chunk, encoding, callback) {
        callback();
    }
    _final(callback) {
        callback();
    }
}
exports.TrmNullTerminator = TrmNullTerminator;
const nullTerminator = () => new TrmNullTerminator();
exports.nullTerminator = nullTerminator;
//# sourceMappingURL=NullTerminator.js.map