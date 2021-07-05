import { Transform } from 'stream';

export class TxTransformKeywordObjects extends Transform {

    transformer:Function;
    postOp:Function;
    accumulator:any;

    constructor( props ) {
        super();
        this.transformer = props.transformer;
        this.postOp = props.postFn || null;
        this.accumulator = props.initialState || [];
    }

    _transform( chunk, encoding, callback ) {

        try {

            if(typeof this.transformer !== 'function')
                throw new Error(`provided transform function is not a function`);

            let data;

            try {
                data = JSON.parse(chunk.toString());
            }
            catch( err ) {
                throw new Error(`failed parsing chunked data.`)
            }

            let result = data.reduce(this.transformer, this.accumulator);
            if(this.postOp)
                result = this.postOp(result);
            
            if(result instanceof Promise) {
                Promise.all([result]).then( arr => {
                    this.accumulator = arr[0];
                    callback();
                })
            }
            else {
                this.accumulator = result;
                callback();
            }

        }
        catch( err ) {
            callback(`TxTransformKeywordObject has failed - ${err.message}`);
        }
    }

    _final( callback ) {
        this.push(JSON.stringify(this.accumulator));
        callback();
    }

}

export const transformKeywordObjects = (options=null) => new TxTransformKeywordObjects(options);