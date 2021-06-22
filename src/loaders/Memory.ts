import {
    Writable
} from 'stream';

interface toGlobalMemoryOptions{  }
export async function toGlobalMemory( options:toGlobalMemoryOptions={} ) {

    class WrGlobalMemory extends Writable {

        callcount = 0;
        rowcount = 0;
        _write( chunk, encoding, callback ) {
            
            try {
                console.log(`write ${this.callcount+1}`);
                this.callcount++;
                callback();
            }
            catch( err ) {
                callback(new Error(`failure!`));
            }
        }

        _final( callback ) {
            
            console.log('final');
            callback();
        }

    }

    const instance = new WrGlobalMemory(options={});
    return instance;
}