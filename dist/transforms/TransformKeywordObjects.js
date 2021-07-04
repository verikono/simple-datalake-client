"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.transformKeywordObjects = exports.TxTransformKeywordObjects = void 0;
const stream_1 = require("stream");
class TxTransformKeywordObjects extends stream_1.Transform {
    constructor(props) {
        super();
        this.transformer = props.transformer;
        this.postOp = props.postFn || null;
        this.initialState = props.initialState || [];
    }
    _transform(chunk, encoding, callback) {
        try {
            if (typeof this.transformer !== 'function')
                throw new Error(`provided transform function is not a function`);
            let data;
            try {
                data = JSON.parse(chunk.toString());
            }
            catch (err) {
                throw new Error(`failed parsing chunked data.`);
            }
            let result = data.reduce(this.transformer, this.initialState);
            if (this.postOp)
                result = this.postOp(result);
            if (result instanceof Promise) {
                Promise.all([result]).then(arr => {
                    this.push(JSON.stringify(arr[0]));
                    callback();
                });
            }
            else {
                this.push(JSON.stringify(result));
                callback();
            }
        }
        catch (err) {
            callback(`TxTransformKeywordObject has failed - ${err.message}`);
        }
    }
}
exports.TxTransformKeywordObjects = TxTransformKeywordObjects;
const transformKeywordObjects = (options = null) => new TxTransformKeywordObjects(options);
exports.transformKeywordObjects = transformKeywordObjects;
//# sourceMappingURL=TransformKeywordObjects.js.map