"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.inspect = exports.TxInspect = void 0;
const stream_1 = require("stream");
class TxInspect extends stream_1.Transform {
    constructor() {
        super(...arguments);
        this.count = 0;
    }
    _transform(chunk, encoding, callback) {
        console.log(this.count, chunk.toString());
        this.count++;
        this.push(chunk);
        callback();
    }
}
exports.TxInspect = TxInspect;
const inspect = (options = null) => new TxInspect(options);
exports.inspect = inspect;
//# sourceMappingURL=Inspect.js.map