import { Transform } from 'stream';

interface TxApplyMutationsProps {
    pk: string|string[]|Function;
    modifications: Array<any>;
}
/**
 * Apply modifications across data represented in key/value objects.
 */
export class TxApplyMutations extends Transform {

    pk;
    modifications;
    // matches=[];

    constructor( options ) {

        super();

        options = options || {};

        this.pk = options.pk;
        this.modifications = options.modifications || [];    
    }

    derivePK( keyed_row ) {

        switch(typeof this.pk) {

            case 'string':
                return keyed_row[this.pk];

            case 'object':
                if(!Array.isArray(this.pk))
                    throw Error(`unable to use argued pk - expected string|string[]|Function - got ${typeof this.pk}`);
                return this.pk.reduce((acc, key) => acc.concat(keyed_row['pk']), '');

            case 'function':
                try {
                    const result = this.pk(keyed_row);
                    if(!result)
                        throw new Error('PK functions must return a truthy result');
                    return result;
                }
                catch( err ) {
                    throw new Error(`pk function thew an error - ${err.message}`)
                }

            default:
                throw new Error(`unable to use argued pk - expected string | string[] | Function - got ${typeof this.pk}`);

        }

    }

    /**
     * Apply a modification if its in the modificaiton stack otherwise return the keyed_row unmodified.
     * 
     * @param keyed_row 
     * @returns 
     */
    applyModifications( keyed_row ) {

        const pk = this.derivePK(keyed_row);
        const mod = this.modifications.find(mod => this.derivePK(mod) === pk) || {};
        return Object.assign(keyed_row, mod);
    }

    _transform( chunk, encoding, callback ) {

        try {

            let data;
            try {
                data = JSON.parse(chunk);
            }
            catch( err ) {
                throw new Error(`failed parsing a chunk from JSON`)
            }


            // const p = JSON.parse(chunk.toString());
            // p.forEach(obj => {
            //     const conflict = this.matches.find(m => m === obj.promo_id)
            //     if(conflict) {
            //         console.log('#######');
            //     }
            //     this.matches.push(obj.promo_id);
            // })
            // console.log('chunk');

            if(Array.isArray(data)) {

                const rows = data.map(row => this.applyModifications(row));
                this.push(JSON.stringify(rows));
            }
            else {
                this.push(this.applyModifications(data));
            }

            callback();            
        }
        catch( err ) {

            callback(new Error(`TxApplyMutations has failed - ${err.message}`));
        }

    }

}

export const applyMutations = (options:TxApplyMutationsProps) => new TxApplyMutations(options);