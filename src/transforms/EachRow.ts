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
    processedFirstRow = false;
    rowNum = 0;
    promises = [];
    meta=null;
    delimiter;
    linebreak;
    delimiterCount=0;
    headings;
    lastPartial;

    constructor( options:EachRowProps ) {

        super();

        this.options = options;
        if(!this.options.parserOptions)
            this.options.parserOptions = {};

    }

    _transform( chunk, encoding, callback ) {

        try {

            const lines = chunk.toString().split(/\r?\n/);

           
            if(!this.processedFirstRow) {
                this.headings = lines[0];
                const parse = Papa.parse(this.headings);
                this.meta = parse.meta;
                this.delimiter = parse.meta.delimiter;
                this.linebreak = parse.meta.linebreak;
                this.delimiterCount = parse.data[0].length;
                this.processedFirstRow = true;
            }
            else {
                lines[0] = this.lastPartial+lines[0];
                lines.unshift(this.headings)
            }

            const endline = lines[lines.length-1];
            const endIsPartial = !(endline.split(this.delimiter).length === this.delimiterCount && endline.substr(-2) === "\n");
            this.lastPartial = endIsPartial ? lines.pop() : '';

            const {data} = Papa.parse(lines.join(this.linebreak), {header:true});

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