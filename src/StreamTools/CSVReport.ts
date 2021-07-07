import {
    Writable
} from 'stream';

interface csvReportProps{  }
export async function csvReport( options:csvReportProps={} ) {

    class WrCSVReport extends Writable {

        lineCount = 0;

        _write( chunk, encoding, callback ) {

            try {
                const lines = chunk.toString().split(/\r?\n/);   
                this.lineCount+=lines.length;        
                callback();    
            }
            catch( err ) {
                callback(new Error(`WrCSVReport has failed - ${err.message}`));
            }
        }

        _final( callback ) {
            
            console.log('CSV Report');
            console.log(`lines:- ${this.lineCount}`)
            callback();
        }

    }

    const instance = new WrCSVReport(options={});
    return instance;
}