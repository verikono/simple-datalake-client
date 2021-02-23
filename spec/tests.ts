require('dotenv').config()

import * as fs from 'fs';
import * as path from 'path';

import { assert } from 'chai';
import {
    describe,
    it
} from 'mocha';

import {
    DefaultAzureCredential
} from '@azure/identity';

import {
    DataLakeServiceClient,
    DataLakeFileClient
} from '@azure/storage-file-datalake';

import {
    AzureDatalakeClient
} from '../src';

import {
    AzureDatalakeExt
} from '../src/ext';


import * as crypto from 'crypto';

describe(`Datalake client tests`, function() {

    this.timeout(120000);

    let instance:AzureDatalakeClient;
    let validURLGunzipped ='https://nusatradeadluat.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/PIZZA-optimization-20201216-0/PIZZA/input/calendar_constraints.csv.gz'
    
    const validURL = process.env.TEST_VALID_URL;
    const validURL_BIG = process.env.TEST_VALID_URL_BIG;
    const validURL_ZIPPED = process.env.TEST_VALID_URL_ZIPPED;
    const validURL_BIG_ZIPPED = process.env.TEST_VALID_URL_BIG_ZIPPED
    const validURLNotExists = validURL.split('/').slice(0, -1).concat('nofilenoway.csv').join('/')

    describe(`Setup`, () => {

        describe(`Environment`, () => {

            it(`Has AZURE_TENANT_ID`, () => Boolean(process.env.AZURE_TENANT_ID));
            it(`Has AZURE_CLIENT_ID`, () => Boolean(process.env.AZURE_CLIENT_ID));
            it(`Has AZURE_CLIENT_SECRET`, () => Boolean(process.env.AZURE_CLIENT_SECRET));

            it(`Has STORAGE_ACCOUNT`, () => Boolean(process.env.STORAGE_ACCOUNT));
            it(`Has STORAGE_ACCOUNT_KEY`, () => Boolean(process.env.STORAGE_ACCOUNT_KEY));
            it(`Has STORAGE_ACCOUNT_SAS`, () => Boolean(process.env.STORAGE_ACCOUNT_SAS));

            it(`Has TEST_VALID_URL`, () => Boolean(validURL));
            it(`Has TEST_VALID_URL_BIG`, () => Boolean(validURL_BIG));
            it(`Has validURL_ZIPPED`, () => Boolean(validURL_ZIPPED));
            it(`Has validURL_BIG_ZIPPED`, () => Boolean(validURL_BIG_ZIPPED));

        })

    });

    describe(`Method Tests`, () => {

        describe(`_parseURL`, () => {

            const instance = new  AzureDatalakeClient();
            const testURL = `https://nusatradeadluat.blob.core.windows.net/dev/working/SNACKING/reference_calendar.csv`
            let result;

            it(`Invokes the method`, () => {
                result = instance._parseURL(testURL);
            });

            it(`Returns a valid protocol`, () => 
                assert(
                    result.hasOwnProperty('protocol') &&
                    result.protocol === 'https',
                    'failed'
                )
            )

            it(`Returns a valid account`, () => 
                assert(
                    result.hasOwnProperty('account') &&
                    result.account === 'nusatradeadluat',
                    'failed'
                )
            )

            it(`Returns a valid host`, () => 
                assert(
                    result.hasOwnProperty('host') &&
                    result.host === 'nusatradeadluat.blob.core.windows.net',
                    'failed'
                )
            );

            it(`Returns a valid storage mechanism`, () => 
                assert(
                    result.hasOwnProperty('storageType') &&
                    result.storageType === 'blob',
                    'failed'
                )
            );

            it(`Returns a valid hostURL`, () => 
                assert(
                    result.hasOwnProperty('hostURL') &&
                    result.hostURL === 'https://nusatradeadluat.blob.core.windows.net',
                    'failed'
                )
            );

            it(`Returns a valid path`, () => 
                assert(
                    result.hasOwnProperty('path') &&
                    result.path === 'dev/working/SNACKING',
                    'failed'
                )
            );

            it(`Returns a valid file`, () => 
                assert(
                    result.hasOwnProperty('file') &&
                    result.file === 'dev/working/SNACKING/reference_calendar.csv',
                    'failed'
                )
            );

            it(`Returns a valid filename`, () => 
                assert(
                    result.hasOwnProperty('filename') &&
                    result.filename === 'reference_calendar.csv',
                    'failed'
                )
            );
          
            it(`Invokes method, handling a malformed/invalid url`, done => {

                try {
                    instance._parseURL(null);
                }
                catch( err ) {
                    done();
                }

            })

        });
        
        describe(`getCredential`, () => {

            const instance = new  AzureDatalakeClient();
            
            it(`invokes getCredential`, () => {

                const result = instance.getCredential()
                assert(result instanceof DefaultAzureCredential, 'failed');
            })

        });

        describe(`getServiceClient`, () => {

            const instance = new  AzureDatalakeClient();
            let result:DataLakeServiceClient;

            it(`invokes the method`, () => {

                result = instance.getServiceClient({
                    url: validURL
                });
            });

            it(`Returned the ServiceClient`, () => {

                assert(result instanceof DataLakeServiceClient, 'failed')
            });

            it(`Cached the client in the instance, keyed to its hosturl`, async () => {

                const { hostURL } = instance._parseURL(validURL)
                assert(
                    instance.serviceClients.hasOwnProperty(hostURL) &&
                    instance.serviceClients[hostURL] instanceof DataLakeServiceClient,
                    'failed'
                );

                let i = 1;
                for await (const fs of result.listFileSystems()) {
                    console.log(`Filesys ${i++} - ${fs.name}`)
                }
            });

        });

        describe(`getFileClient`, () => {

            const instance = new  AzureDatalakeClient();
            let result:DataLakeFileClient;

            it(`invokes the method`, () => {

                result = instance.getFileClient({url: validURL});
                assert(result instanceof DataLakeFileClient, 'failed');
            });


        });

        describe(`exists`, () => {

            const instance = new AzureDatalakeClient();
            let result:boolean;

            it(`invokes the method on a known existing url`, async () => {

                result = await instance.exists({url: validURL});
                assert(result === true, 'failed');
            });

            it(`invokes the method on a known non-existing url`, async () => {

                result = await instance.exists({url: validURLNotExists});
                assert(result === false, 'failed');
            });


        });

        describe(`stream`, () => {

            const instance = new AzureDatalakeClient();

            it(`Invokes stream`, async () => {

                let receivedData = false;
                let receivedEnd = false;

                const result = await instance.stream({
                    url: validURL,
                    onData: data => {
                        receivedData = true;
                    },
                    onEnd: data => {
                        receivedEnd = true;
                    }
                });

                assert(receivedData, 'failed - expected onData to have been invoked but was not');
                assert(receivedEnd, 'failed - expected onEnd to have been invoked but was not');

            });

        });

        describe(`get`, () => {

            const instance = new AzureDatalakeClient();

            it(`Invokes get upon a valid URL`, async () => {
                const result = await instance.get({url: validURL});
                assert(typeof result === 'string' && result.length > 1, 'failed');
            });
        });

        describe(`save`, () => {

            const instance = new AzureDatalakeClient();

            it(`invokes save upon a valid URL`, async () => {

                const file = '_sub1/_sub2/tests.csv'
                const result = await instance.save({url: validURL, file});
                assert(fs.existsSync(file), 'file does not exist');
                fs.unlinkSync(file);
                fs.rmdirSync('_sub1/_sub2');
            });

            it(`Sets the default save path and invoke save upon a valid URL`, async () => {

                const file = '_sub3/tests.csv';
                const rootdir = '_sub1'
                instance.setSaveRoot({path: rootdir});
                const result = await instance.save({url: validURL, file});
                assert(fs.existsSync(path.join(rootdir, file)))
                fs.unlinkSync(path.join(rootdir, file))
                fs.rmdirSync('_sub1/_sub3');
                fs.rmdirSync('_sub1');
            });



        });

    });

    describe.only(`Extensions Tests`, () => {

        it(`Extensions exist on instance`, () => {

            const instance = new AzureDatalakeClient();
            assert(instance.ext instanceof AzureDatalakeExt, 'failed');
        });

        describe(`count`, () => {

            const instance = new AzureDatalakeClient();
 
            it(`invokes count upon a valid URL`, async () => {

                const result = await instance.ext.count({url: validURL});
                assert(result && typeof result === 'number' && result > 1, 'failed');
            });

            it(`invokes count upon a valid URL`, async () => {

                const result = await instance.ext.count({url: validURL_ZIPPED});
                assert(result && typeof result === 'number' && result > 1, 'failed');
            });

        });

        describe(`reduce`, () => {

            const instance = new AzureDatalakeClient();

            it(`invokes reduce with a valid URL`, async () => {
                
                const cnt = await instance.ext.count({url: validURL});
                const result = await instance.ext.reduce({
                    url: validURL,
                    reducer: acc => acc+1,
                    accumulator: 0
                });

                //cnt-1 because a CSV has 1 header row.
                assert(cnt-1 === result, 'failed');

            });

            it(`invokes reduce with an invalid URL`, async () => {

                let receievedError = false;
                try {
                    await instance.ext.reduce({url: validURLNotExists, accumulator: 0, reducer: () => {}});
                }
                catch( err ) {
                    receievedError = true;
                }

                assert(receievedError, 'failed - did not get the expected error');

            });

            it(`invokes reducer with a valid GZIPPED URL`, async () => {

                let cnt = await instance.ext.count({url: validURL});
                const result = await instance.ext.reduce({
                    url: validURL_ZIPPED,
                    reducer: (acc, data) => acc+1,
                    accumulator: 0
                });
                //cnt-1 because a CSV has 1 header row.
                assert(cnt-1 === result, 'failed')
            });

            it(`gracefully errors when a problem occurs in the reducer`, async () => {

                try {
                    //invokes Array.push on a number.
                    await instance.ext.reduce({
                        url: validURL,
                        reducer: (acc, data) => acc.push(3),
                        accumulator: 0
                    })
                }
                catch( err ){

                    assert(err && err.message.includes('not a function'), 'failed')
                }
            })

        });

        describe(`map`, () => {

            const instance = new AzureDatalakeClient();

            it(`invokes map with a valid URL`, async () => {

                const cnt = await instance.ext.count({url: validURL});
                const result = await instance.ext.map({url: validURL, mapper: (data, i) => {
                    return i;
                }});

                assert(cnt-1 === result.length, 'failed');
            });

            it(`gracefully errors when a problem occurs in the reducer`, async () => {

                try {
                    //invokes Array.push on a number.
                    await instance.ext.map({
                        url: validURL,
                        mapper: (acc, data) => data.somevar.thatdoesntexist
                    })
                }
                catch( err ){

                    assert(err && err.message.includes('Cannot read property'), 'failed')
                }
            })

        });

        describe(`forEach`, () => {

            const instance = new AzureDatalakeClient();

            it(`invokes forEach with the validURL`, async () => {

                //we need the row nums
                const count = await instance.ext.count({url: validURL});

                let cnt = 0;
                const result = await instance.ext.forEach({url: validURL, fn: (data, i) => {
                    cnt++;
                }});

                //minus 1 because row 0 is a columns title row
                assert(cnt === (count-1), 'failed');

            });

            it(`invokes forEach with the invalidURL`, async () => {

                let receievedError = false;
                try {
                    await instance.ext.forEach({url: validURLNotExists, fn: () => {}});
                }
                catch( err ) {
                    receievedError = true;
                }

                assert(receievedError, 'failed');
            });

            it(`invokes forEach with a validURL , nonblocking`, async () => {

                //we need the row nums
                const count = await instance.ext.count({url: validURL});

                let cnt = 0;
                let invoked = false;
                const result = await instance.ext.forEach({url: validURL, block: false, fn: (data, i) => {
                    cnt++;
                    invoked = true;
                }});

                let afterInvokeCount = cnt;
                assert(afterInvokeCount !== count, 'failed');

                await new Promise(r => setTimeout(e=> {
                    assert(invoked, 'failed');
                    r(true);
                }, 100))
            });

            it(`invokes forEach with a valid Gzipped URL`, async () => {
                
                let cnt = 0;
                const result = await instance.ext.forEach({url: validURL_ZIPPED, fn: (data, i) => {
                    cnt++;
                }});
            })

        });

        describe.skip(`cache`, () => {

            it(`Caches`, async () => {

                const instance = new AzureDatalakeClient();
                const result = await instance.ext.cache({
                    url: validURL,
                    table: 'brenstest',
                    partitionKey: 'planning_account',
                    rowKey: row =>  crypto.createHash('md5').update(JSON.stringify(row)).digest("hex")
                }, {delimiter:'|'});

                const count = await instance.ext.count({url: validURL}, {delimiter:'|'})
                assert(count === result.numRowsInserted, 'failed');
            });

        });

        describe(`mapSlices`, () => {

            const instance = new AzureDatalakeClient();

            it(`invokes mapSlices on `, async () => {

                const slicesize = 50;
                const cnt = await instance.ext.count({url: validURL});
                let totalRows = 0;

                const result = await instance.ext.mapSlices({
                    url: validURL,
                    size: slicesize,
                    mapper: (rows, i) => {
                        totalRows = totalRows + rows.length;
                        assert(rows.length <= slicesize, 'failed -slicesize to big')
                    }
                }, {delimiter:'|'});

                assert(totalRows === cnt-1, 'failed');
            });

        });

    });

    describe.skip(`Single File Test`, () => {

        it(`Tests a reduce on a given file`, async () => {

        });

    });

});