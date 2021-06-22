import { Transform } from 'stream';

export class TxInspect extends Transform {

    count=0;
    
     _transform( chunk, encoding, callback ) {

        console.log(this.count, chunk.toString())
        this.count++;
        this.push(chunk);
        callback();
    }

}

export const inspect = (options=null) => new TxInspect(options);