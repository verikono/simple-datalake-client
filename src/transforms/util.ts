import * as Papa from 'papaparse';

/**
 * determine what the chunk is; it could be a stream, a keywordObject Array.
 * @param chunk 
 * @returns 
 */
export function chunkType( chunk ) {

    try{

        if(!(chunk instanceof Buffer))
            throw new Error(`argued chunk should be a buffer`)

        const str = chunk.toString();

        //determine if its JSON
        if(str[0] === '[' || str[0] === '{') {
            try {
                JSON.parse(str);
                return 'JSON';
            }
            catch( err ) {
                //do nothing, its not JSON.
            }
        }

        //is it CSV
        const lines = chunk.toString().split(/\r?\n/);
        try {
            const parse = Papa.parse(lines[0]);
            if(parse.meta.delimiter)
                return 'CSV';
        }
        catch( err ) {

        }

        throw new Error(`failed to determine stream type - not CSV nor JSON`);

    }
    catch( err ) {

        throw new Error(`AzureDatalakeClient::util.chunkType has failed - ${err.message}`);
    }
}

export function isEmptyObject( obj ) {

    try {
        return Object.keys(obj).length === 0;
    }
    catch(err) {
        return false;
    }
}