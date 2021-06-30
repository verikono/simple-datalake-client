import { Transform } from 'stream';
import * as Papa from 'papaparse';

interface EachRowProps {
    parserOptions?:any; //https://www.papaparse.com/docs#config
    onRow?:Function;
    onError?:Function;
    onEnd?:Function;
}

export class TxEachRow extends Transform {

    options:EachRowProps;
    rowNum = 0;
    promises = [];
    meta=null;

    constructor( options:EachRowProps ) {

        super();

        this.options = options;
        if(!this.options.parserOptions)
            this.options.parserOptions = {};

    }

    _transform( chunk, encoding, callback ) {

        try {

            const {
                data,
                errors,
                meta
            } = Papa.parse(chunk.toString(), this.options.parserOptions);

            this.meta = meta;

            if(this.options.onRow) {
                data.forEach(data => {
                    const result = this.options.onRow(data, this.rowNum);
                    this.rowNum++;
                    if(result instanceof Promise)
                        this.promises.push(result);
                });
            }

            this.push(chunk);
            callback();
        }
        catch( err ) {

            callback(new Error(`TxEachRow has failed - ${err.message}`));
        }

    }

    _final( callback ) {

        new Promise((resolve, reject) => {

            if(this.promises.length) {
                Promise.all(this.promises)
                    .then(() => resolve(true))
                    .catch(err => {
                        reject(new Error(`TxEachRow received an error from one of its mappers - ${err.message}`))
                    })
            }
            else{
                resolve(true)
            }
        })
        .then(() => {
            if(this.options.onEnd) {
                const endResult = this.options.onEnd(this.meta);
                if(endResult instanceof Promise) {
                    Promise.all([endResult]).then(() => {
                        callback()
                    });
                }
                else {
                    callback();
                }
            }
        })

    }

}

export const eachRow = (options:EachRowProps = {}) => new TxEachRow(options); 