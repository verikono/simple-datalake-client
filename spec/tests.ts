require('dotenv').config()

import * as fs from 'fs';
import * as path from 'path';
import { EOL } from 'os';

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
    AzureDataTablesClient
} from 'verikono-azure-datatable-tools';

import {
    AzureDatalakeClient
} from '../src';

import {
    AzureDatalakeExt
} from '../src/ext';

import { pipeline } from 'stream';

import {
    CSVStreamToKeywordObjects,
    keywordObjectsToArray,
    keywordArrayToCSV,
    applyMutations,
    inspect,
    applyAzureKeys
} from '../src/transforms';

import {
    fromAzureDatalake,
    toAzureDatalake,
    toGlobalMemory
} from '../src/loaders';

import {
    csvReport
} from '../src/StreamTools';

import * as crypto from 'crypto';

import * as zlib from 'zlib';
import { toAzureDataTables } from '../src/terminators';

describe(`Datalake client tests`, function() {

    this.timeout(1200000);

    let instance:AzureDatalakeClient;
    let validURLGunzipped ='https://nusatradeadluat.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/PIZZA-optimization-20201216-0/PIZZA/input/calendar_constraints.csv.gz'
    
    const parsableURL = 'https://someaccount.blob.core.windows.net/container/directory/file.csv';
    const validURL = process.env.TEST_VALID_URL;
    const validURL_BIG = process.env.TEST_VALID_URL_BIG;
    const validURL_ZIPPED = process.env.TEST_VALID_URL_ZIPPED;
    const validURL_BIG_ZIPPED = process.env.TEST_VALID_URL_BIG_ZIPPED;
    const validURL_WITH_EMPTY_COLUMNS = process.env.TEST_VALID_URL_WITH_EMPTY_COLUMNS;
    const validURL_DIRECTORY = process.env.TEST_VALID_DIRECTORY_URL;
    const touchTestUrl = process.env.TEST_TOUCH_URL;

    const validURLNotExists = validURL.split('/').slice(0, -1).concat('nofilenoway.csv').join('/')
    let validReferenceCalenderURL = process.env.TEST_VALID_REFERENCE_CALENDAR_URL;

    describe(`Setup`, () => {

        describe.only(`Environment`, () => {

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
            it(`Has validURL_DIRECTORY`, () => Boolean(validURL_DIRECTORY));
        })

    });

    describe(`Method Tests`, () => {

        describe(`_parseURL`, () => {

            const instance = new  AzureDatalakeClient();
            let result;

            it(`Invokes the method`, () => {

                result = instance._parseURL(parsableURL);
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
                    result.account === 'someaccount',
                    'failed'
                )
            )

            it(`Returns a valid host`, () => 
                assert(
                    result.hasOwnProperty('host') &&
                    result.host === 'someaccount.blob.core.windows.net',
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
                    result.hostURL === 'https://someaccount.blob.core.windows.net',
                    'failed'
                )
            );

            it(`Returns a valid path`, () => 
                assert(
                    result.hasOwnProperty('path') &&
                    result.path === 'container/directory',
                    'failed'
                )
            );

            it(`Returns a valid file`, () => 
                assert(
                    result.hasOwnProperty('file') &&
                    result.file === 'directory/file.csv',
                    'failed'
                )
            );

            it(`Returns a valid filename`, () => 
                assert(
                    result.hasOwnProperty('filename') &&
                    result.filename === 'file.csv',
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
        
        describe('_isURLDirectory', async () => {

            it(`Correctly identifies a valid directory URL`, async () => {
                
                const instance = new AzureDatalakeClient();
                const directoryIsDirectory = await instance._isURLDirectory(validURL_DIRECTORY);

                assert(directoryIsDirectory === true, 'failed');
            });

            it(`Correctly identifies a valid file URL`, async () => {

                const instance = new AzureDatalakeClient();
                const filesIsDirectory = await instance._isURLDirectory(validURL);
                assert(filesIsDirectory === false, 'failed');
            });

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

        describe(`get`, () => {

            it(`Invokes get upon a valid URL`, async () => {

                const instance = new AzureDatalakeClient();
                const result = await instance.get({url: validURL_DIRECTORY});
            });
        });

        describe(`touch && delete (both because we create and delete, and test for both here.)`, () => {

            it(`touches a file to the base of the container`, async () => {

                const {
                    TEST_TOUCH_URL
                } = process.env;

                assert(Boolean(TEST_TOUCH_URL), 'cannot test - please set TEST_TOUCH_URL in your environment to be a datalake URL where no file exists');
                const instance = new AzureDatalakeClient();
                const result = await instance.touch({url: TEST_TOUCH_URL});
                assert(result === true, 'failed - expected a true to return');
                assert(await instance.exists({url: TEST_TOUCH_URL}), `failed - file does not exist at ${TEST_TOUCH_URL}`);
                try {
                    await instance.delete({url: TEST_TOUCH_URL});
                    assert(!(await instance.exists({url: TEST_TOUCH_URL})), `test cleanup failed - file still exists , please delete ${TEST_TOUCH_URL}`);
                }
                catch( err ) {
                    throw new Error(`test cleanup failed - manually delete ${TEST_TOUCH_URL} pleaseum - ${err.message}`);
                }
            });

            it(`touches a file with some precursive directory structure`, async () => {

                const {
                    TEST_TOUCH_DIR_URL,
                    TEST_TOUCH_BASEDIR_URL
                } = process.env;

                //example of process envs
                //TEST_TOUCH_DIR_URL : https://myaccount.blob.mic.net/container/testdirectory/touchtestdir1/touchtest2/file.txt
                //TEST_TOUCH_BASEDIR_URL : https//myaccount.blob.mic.net/container/testdirectory/touchtestdir
                //the BASEDIR is used to delete the directory structure during cleanup.

                assert(Boolean(TEST_TOUCH_DIR_URL), 'cannot test - please set TEST_TOUCH_DIR_URL in your environment to be a datalake URL where no file exists and has some precursive directory structure');
                assert(Boolean(TEST_TOUCH_BASEDIR_URL), 'cannot test - please set TEST_TOUCH_BASEDIR_URL in your environment to be a datalake URL where no file exists and has some precursive directory structure');
                
                const instance = new AzureDatalakeClient();
                const result = await instance.touch({url: TEST_TOUCH_DIR_URL});
                assert(result === true, 'failed - expected a true to return');
                assert(await instance.exists({url: TEST_TOUCH_DIR_URL}), `failed - file does not exist at ${TEST_TOUCH_DIR_URL}`);
                try {
                    await instance.delete({url: TEST_TOUCH_BASEDIR_URL});
                    assert(!(await instance.exists({url: TEST_TOUCH_DIR_URL})), `test cleanup failed - file still exists , please delete ${TEST_TOUCH_DIR_URL}`);
                    assert(!(await instance.exists({url: TEST_TOUCH_BASEDIR_URL})), `test cleanup failed - base dir still exists , please delete ${TEST_TOUCH_DIR_URL}`);
 
                }
                catch( err ) {
                    throw new Error(`test cleanup failed - manually delete ${TEST_TOUCH_DIR_URL} pleaseum - ${err.message}`);
                }
            });

        });

        describe(`delete`, () => {

        });

        describe(`list - list contents of a directory`, () => {

            it(`It lists the contents of a known directory`, async () => {

                const instance = new AzureDatalakeClient();
                const result = await instance.list({url: validURL_DIRECTORY});
                assert(Array.isArray(result) && result.length, `failed - received either no array or an empty array - are there files at ${validURL_DIRECTORY}?`)
            });
        });

        describe.skip(`stream`, () => {

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

        describe.skip(`save`, () => {

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

        describe.skip('copy', () => {

            it(`copies a file`, async () => {

                const instance = new  AzureDatalakeClient();

                const source = process.env.TEST_VALID_FILE_URL;
                const target = process.env.TEST_VALID_FILE_TARGET;

                assert((await instance.exists({url: source})) === true, 'failed - source file does not exist to perform test with');

                const result = await instance.copy({source, target});

                assert(result === true, 'failed - expected copy to return true');

                assert((await instance.exists({url: target})) === true, 'failed - target does not appear to exist');

            });

            it(`copies a directory`, async () => {

                const instance = new  AzureDatalakeClient();

                const source = process.env.TEST_VALID_DIRECTORY_URL;
                const target = process.env.TEST_VALID_DIRECTORY_TARGET;

                assert((await instance.exists({url: source})) === true, 'failed - source file does not exist to perform test with');

                const result = await instance.copy({source, target});

                assert(result === true, 'failed - expected copy to return true');
                assert((await instance.exists({url: target})) === true, 'failed - target does not appear to exist');


            });

            it(`copies 3 large files simulatenously`, async () => {

                const instance = new  AzureDatalakeClient();

                const largeFile1 = process.env.TEST_VALID_LARGE_FILE1;
                const largeFile2 = process.env.TEST_VALID_LARGE_FILE2;
                const largeFile3 = process.env.TEST_VALID_LARGE_FILE3;

                const existPromises = [];
                [largeFile1, largeFile2, largeFile3].forEach(url => {
                    existPromises.push(instance.exists({url}))
                })
                await Promise.all(existPromises);
                existPromises.forEach((result,i) => {
                    assert(result === true, `failed - ${i+1} does not exist`);
                })
            });

        })

    });

    describe(`Extensions Tests`, () => {

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

            it(`invokes map with a valid CSV URL `, async () => {

                const cnt = await instance.ext.count({url: validURL});
                const result = await instance.ext.map({url: validURL, mapper: (data, i) => {
                    return i;
                }});

                assert(cnt-1 === result.length, 'failed');
            });

            it(`invokes map with a valid pipe seperated "csv" URL, autodetecting the delimiter`, async () => {

                const { TEST_VALID_PSV_URL } = process.env;
                const cnt = await instance.ext.count({url: TEST_VALID_PSV_URL });
                const result = await instance.ext.map({url: TEST_VALID_PSV_URL, mapper: (data, i) => {
                    return i;
                }})
                assert(result.length, 'failed - no results');
                assert(result.length === cnt, 'failed - result should equal count');

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
            });

            it(`Persists the result upon the file it parsed and mapped`, async () => {

                let result;

                const url = 'https://nusatradeadl.blob.core.windows.net/dev/test/testfile.csv';

                const mock = [
                    'val1,val2',
                    'a,b',
                    'aa,bb',
                    'aaa,bbb'
                ];

                const instance = new AzureDatalakeClient();
                
                const iterationCount = mock.length;
                let currentIteration = 0;

                result = await instance.put({url, content: mock.join('\n'), overwrite:true});                
                assert(result === true, `failed uploading mock data to ${url}`);

                const original = await instance.get({url});

                result = await instance.ext.map(
                    {
                        url,
                        persist:true,
                        mapper: (row, itr) => {
                            row.val1 = row.val1+'!'
                            row.val2 = row.val2+'!!'
                            currentIteration = itr;
                            return row;
                        }
                    }
                );
                assert(Array.isArray(result), 'expected the result to be returned from the map operation');
                //minus 1 for the heading row and minus another 1 for the zero count vs human count
                assert(iterationCount === currentIteration + 1 + 1, 'did not loop through all rows of the mock');

                const mutated = await instance.ext.get({url});

                assert(JSON.stringify(mutated) === JSON.stringify(result), 'failed - expected the stored version to be precisely the same as the result');

                


            });

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

        describe(`cache`, () => {

            it(`Caches`, async () => {

                const table = 'brentest';
                const url = validReferenceCalenderURL;

                const instance = new AzureDatalakeClient();
                const result = await instance.ext.cache({
                    url: validReferenceCalenderURL,
                    table,
                    partitionKey: 'planning_account',
                    rowKey: ['group_name', 'duration', 'start_date', 'promo_tactic'],
                    replaceIfExists:true
                });

                const count = await instance.ext.count({url: validReferenceCalenderURL})
                assert(count === result.numRowsInserted, `failed - expected ${count} rows to exist but got ${result.numRowsInserted}`);
            
                try {

                    const tables = new AzureDataTablesClient();
                    await tables.drop({table});
                }
                catch( err ) {
                    throw new Error(`whilst the test passed - cleanup was not successfull - please manually remove ${table}.`);
                }
            });

            it(`Caches with types`, async () => {

                const tableName = 'cacheTypesTest'
                let result;

                const instance = new AzureDatalakeClient();

                //@todo remove the global keys once this package is compatible with AzureDefaultCredential.
                const tables = new AzureDataTablesClient();

                result = await instance.ext.cache({
                    url: validReferenceCalenderURL,
                    table: tableName,
                    partitionKey: 'planning_account',
                    rowKey: row => {
                        return crypto.createHash('md5').update(JSON.stringify(row)).digest('hex');
                    },
                    replaceIfExists:true,
                    types: {
                        duration: "float",
                        cust_promo_id: "string",
                        feature_ind: "boolean",
                        bb_unit_rate: "float"
                    }
                });

                assert(result && result.numRowsInserted > 0, 'failed');
                const {numRowsInserted} = result;

                result = await tables.rows({table: tableName});
                await tables.drop({table: tableName});

                assert(result.length === numRowsInserted, 'failed');
                assert(result.every(row => typeof row.bb_unit_rate === 'number'), 'failed');
                assert(result.every(row => typeof row.cust_promo_id === 'string'), 'failed');

                
            });

            it(`Errors appropriately`, () => {
               
                const instance = new AzureDatalakeClient();

                return new Promise( async (resolve, reject) => {

                    instance.ext.cache({
                        url: validReferenceCalenderURL,
                        table: 'wontmatter',
                        partitionKey: (v) => {throw new Error('do the error')},
                        rowKey: ['group_name', 'duration', 'start_date', 'promo_tactic'],
                        replaceIfExists:true
                    })
                    .then(result => reject(Error(`Did not throw the expected error`)))
                    .catch(err => resolve(true))
                });

            });

            it(`Callsback on recently deleted tables, taking longer but actually getting the job done`, async () => {

                const table = 'cacheondroppedtables';
                const instance = new AzureDatalakeClient();
                const tables = new AzureDataTablesClient();

                let result;
                result = await instance.ext.cache({
                    url: validReferenceCalenderURL,
                    table,
                    partitionKey: 'planning_account',
                    rowKey: ['group_name', 'duration', 'start_date', 'promo_tactic'],
                    replaceIfExists:true
                });

                await tables.drop({table});

                //we expect this to take at least 30 seconds (so the library can await the operations queue)
                result = await instance.ext.cache({
                    url: validReferenceCalenderURL,
                    table,
                    partitionKey: 'planning_account',
                    rowKey: ['group_name', 'duration', 'start_date', 'promo_tactic'],
                    replaceIfExists:true
                });

                const exists = await tables.existsAndHasData({table});
                assert(exists === true, 'failed');
                await tables.drop({table});

            });

        });

        describe(`mapSlices`, () => {

            it(`invokes mapSlices on `, async () => {

                const instance = new AzureDatalakeClient();

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

            it.skip(`invokes mapSlices and nullifies empty columns`, async () => {

                const instance = new AzureDatalakeClient();

                const url = validURL_WITH_EMPTY_COLUMNS;
                const exists = await instance.exists({url});
                if(!exists)
                    throw Error(`Test file does not exist`);

                const control = await instance.ext.mapSlices({
                    url,
                    mapper: (rows, i) => {
                        return rows;
                    }
                }, { delimiter:'|'});

                const result = await instance.ext.mapSlices({
                    url,
                    mapper: (rows, i) => {
                        return rows;
                    }
                }, { delimiter:'|', nullifyEmptyColumns:true});

                const nullFoundInControl = control.some(row => {
                    return Object.values(row).some(value => value === null)
                });

                const nullFoundInData = result.some(row => {
                    return Object.values(row).some(value => value === null)
                });

                assert(nullFoundInData && !nullFoundInControl, 'failed');
            })
        });

        describe(`get`, () => {

            it(`invokes get on a valid URL`, async () => {

                const instance = new AzureDatalakeClient();

                const url = "https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/1ca34a07-fea3-4a92-8d53-c74cc90a295d/BAKING/input/reference_calendar.csv.gz"; //validURL;

                const delimiter = "|";
                const exists = await instance.exists({url});
                if(!exists)
                    throw Error(`Test file does not exist`);

                const result = await instance.ext.get({url},{delimiter});

                assert(result && Array.isArray(result) && result.length, 'failed');
            });

        })

        describe(`compile`, async () => {

            it(`invokes get on valid URLs`, async () => {

                const instance = new AzureDatalakeClient();
                const { data, diff } = await instance.ext.compile({
                    urls: [
                        'https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/62b5fcda72cd43958cdc4205ed9376c5/COFFEE%20PARTNERS/output/optimized_simulated.csv.gz',
                        'https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/rhelsen%40enterrasolutions.com/3aefc3909d664dcf95a47a72ce8a0286/COFFEE%20PARTNERS/output/optimized_simulated.csv.gz'
                    ],
                    pk: data => {
                        return ['planning_account', 'start_date', 'group_name', 'promo_tactic']
                                .map(key => data[key])
                                .join('|');
                    }
                }, {delimiter:'|'});

                assert(Array.isArray(data), 'data is spuposed to be an array');
                assert(data.length, 'no rows were returned');
                assert(Object.keys(diff).length > 0, 'received an unpopulated diff where changes were expected to return');

            });

            it(`errors gracefully when a non-existant URL is provided`, async () => {

                let caught = false;
                try {

                    const instance = new AzureDatalakeClient();
                    const { data, diff } = await instance.ext.compile({
                        urls: [
                            'https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/62b5fcda72cd43958cdc4205ed9376c5/COFFEE%20PARTNERS/output/doesnotexist.csv.gz'
                        ],
                        pk: data => {
                            return ['planning_account', 'start_date', 'group_name', 'promo_tactic']
                                    .map(key => data[key])
                                    .join('|');
                        }
                    }, {delimiter:'|'});

                }
                catch( err ) {
                    caught = true;
                }
                assert(caught === true, 'failed');


            });

            it(`errors gracefully when a pk function errors`, async () => {

                const instance = new AzureDatalakeClient();
                let caught = false;
                try {

                    await instance.ext.compile({
                        urls: ["https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/62b5fcda72cd43958cdc4205ed9376c5/COFFEE PARTNERS/output/optimized_simulated.csv.gz"],
                        pk: data => {
                            throw new Error('synthetic testing error')
                        }
                    });
                }
                catch( err ) {

                    caught = true;
                }
                assert(caught === true, 'failed');
                
            });

            it(`errors gracefully when a PK function returns a falsy result`, async () => {

                const instance = new AzureDatalakeClient();
                let caught = false;
                try {

                    await instance.ext.compile({
                        urls: ["https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/62b5fcda72cd43958cdc4205ed9376c5/COFFEE PARTNERS/output/optimized_simulated.csv.gz"],
                        pk: data => {
                            return false;
                        }
                    });
                }
                catch( err ) {

                    caught = true;
                }
                assert(caught === true, 'failed');
                
            });

            it(`works with 1 url provided - returning the data and an empty diff`, async () => {

                const instance = new AzureDatalakeClient();
                const { data, diff } = await instance.ext.compile({
                    urls: [
                        "https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/62b5fcda72cd43958cdc4205ed9376c5/COFFEE PARTNERS/output/optimized_simulated.csv.gz",
                        //'https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/62b5fcda72cd43958cdc4205ed9376c5/COFFEE%20PARTNERS/output/optimized_simulated.csv.gz'
                    ],
                    pk: data => {
                        return ['planning_account', 'start_date', 'group_name', 'promo_tactic']
                                .map(key => data[key])
                                .join('|');
                    }
                }, {delimiter:'|'});

                assert(Array.isArray(data), 'data is spuposed to be an array');
                assert(data.length, 'no rows were returned');
                assert(Object.keys(diff).length === 0, 'received a populated diff where the diff was expected to be empty');
            });

            it(`Records no changes when the same URL is used 3 times`, async () => {

                const instance = new AzureDatalakeClient();
                const { data, diff } = await instance.ext.compile({
                    urls: [
                        'https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/62b5fcda72cd43958cdc4205ed9376c5/COFFEE%20PARTNERS/output/optimized_simulated.csv.gz',
                        'https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/62b5fcda72cd43958cdc4205ed9376c5/COFFEE%20PARTNERS/output/optimized_simulated.csv.gz',
                        'https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/62b5fcda72cd43958cdc4205ed9376c5/COFFEE%20PARTNERS/output/optimized_simulated.csv.gz'
                    ],
                    pk: data => {
                        return ['planning_account', 'start_date', 'group_name', 'promo_tactic']
                                .map(key => data[key])
                                .join('|');
                    }
                }, {delimiter:'|'});

                assert(Array.isArray(data), 'data is spuposed to be an array');
                assert(data.length, 'no rows were returned');
                assert(Object.keys(diff).length === 0, 'received a populated diff where the diff was expected to be empty');
            });

            it(`Compiles nulls to new columns, allowing a change to appear in the diff`, async () => {

                const instance = new AzureDatalakeClient();
                const { data, diff } = await instance.ext.compile({
                    urls: [
                        "https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/d77f8a50-ce4d-43d7-a429-1048254a4398/BAKING/output/optimized_simulated.csv.gz",
                        "https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/SYSTEM/d77f8a50-ce4d-43d7-a429-1048254a4398/BAKING/input/reference_calendar.csv.gz"
                    ],
                    pk: data => {
                        return ['planning_account', 'start_date', 'group_name', 'promo_tactic']
                                .map(key => data[key])
                                .join('|');
                    }
                }, {delimiter:'|'});
            });

        });

        describe(`modify`, async () => {

            it(`it applies a modification to some existing datafile`, async () => {

                const url = 'https://nusatradeadl.blob.core.windows.net/dev/test/reference_calendar.csv.gz';
                const instance = new AzureDatalakeClient();
                let result;

                const modification = {
                    planning_account: "AWG KC - Combined",
                    start_date: "20210103",
                    group_name: "USSS2S208S208C2",
                    promo_tactic: "EDLP",
                    duration: "999"
                };

                result = await instance.ext.modify({
                    url: 'https://nusatradeadl.blob.core.windows.net/dev/test/reference_calendar.csv.gz',
                    pk: data => {
                        return ['planning_account', 'start_date', 'group_name', 'promo_tactic']
                                .map(key => data[key])
                                .join('|');
                    },
                    modifications: [modification]
                }, { delimiter:'|' });

                assert(!!result, 'failed - did not get a report as the result');
                assert(Array.isArray(result.modifications) && result.modifications.length === 1, 'failed - invalid modifications returned, expected array and exactly 1 in length');
                assert(Array.isArray(result.modified) && result.modified.length === 1, 'failed - invalid prop modified returned, expected array and exactly 1 in length');
                assert(result.rowsExpectedForModification === 1, 'failed - expected prop rowsExpectedForModificaiton to be 1');
                assert(result.rowsModified === 1, 'failed - expected rowsModified to be 1');
                assert(result.rowsProcessed && typeof result.rowsProcessed === 'number' && result.rowsProcessed > 1, 'failed - expected prop rowsProcessed to be a number greater than 1');
                assert(result.success === true, 'failed - did not indicate the process was successful');

                let found = false;

                result = await instance.ext.forEach({
                    url,
                    fn: (row,i) => {
                        if(Object.keys(modification).every(key => modification[key] === row[key])) {
                            found = true;
                        }
                    }
                },
                {
                    delimiter: '|'
                });

                //@ts-ignore - found can be true, typescript has an issue .
                assert(found === true, 'failed');
            });
        });

        describe(`addNewColumns`, async () => {

            it.only(`adds new columns to an existing CSV datafile`, async () => {

                const url = 'https://nusatradeadl.blob.core.windows.net/dev/test/reference_calendar.csv.gz';
                const testUrl = 'https://nusatradeadl.blob.core.windows.net/dev/test/reference_calendar_addcolstest.csv.gz';
                const instance = new AzureDatalakeClient();

                let result;

                if(!(await instance.exists({url: testUrl}))) {
                    result = await instance.copy({source:url, target:testUrl});
                    assert(result === true, `failed - could not copy ${url} to ${testUrl}`)
                }

                result = await instance.ext.addNewColumns({
                    url:testUrl,
                    columns: {
                        'modifications': '{}',
                        'mo': 'adib'
                    }
                })

                const data = await instance.ext.get({url: testUrl});
                assert(data[0].hasOwnProperty('modifications') && data[0].modifications === '{}', 'failed could not find property modifications');
                assert(data[0].hasOwnProperty('mo') && data[0].mo === 'adib', 'failed could not find property mo');

                result = await instance.delete({url: testUrl});
                assert(result === true, `failed - deletion of test data failed - please delete ${testUrl}`);

            });

        });

        describe(`etl`, async () => {

            it(`develops`, async () => {

                const instance = new AzureDatalakeClient();

                const hashString = str => crypto.createHash('md5').update(str).digest('hex');
                const supercategory = 'test';
                
                const result = await instance.ext.etl({
                    url: 'https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/bnorris@enterrasolutions.com/89b015ae-f8f1-40a2-8e44-fcdc32945f42/BAKING/input/scope.csv.gz',
                    target: 'brentestetl',
                    overwrite:true,
                    transform: (acc, row) => {
                        const hash = hashString(`${row.planning_account}scope`);
                        let constraint = acc.find(row => row.uid === hash);

                        if(!constraint) {
                            constraint = {
                                partitionKey: 'global',
                                rowKey: hash,
                                uid: hash,
                                supercategory,
                                planning_account: row.planning_account,
                                constraint_type: 'scope',
                                constraint_value: [],
                                level: 'planning_account',
                                level_id: row.planning_account,
                                level_description: '',
                                level_name: row.planning_account,
                                hierarchy: []
                            }
                            acc.push(constraint)
                        }

                        constraint.constraint_value.push(row.group_name);
                        return acc;
                    },
                    postFn: async data => {
                        return data.map(constraint => {
                            constraint.constraint_value = typeof constraint.constraint_value === 'string' ? JSON.stringify(constraint.constraint_value) : constraint.constraint_value;
                            constraint.hierarchy = typeof constraint.hierarchy ? JSON.stringify(constraint.hierarchy) : constraint.hierarchy;
                            return constraint;
                        });
                    }
                });

            });


            it(`develops with more data`, async () => {


                const PROMO_CONSTRAINTS = [
                    'min_price',
                    'min_discount',
                    'max_discount',
                    'price_points',
                    'discount_levels',
                    'min_retailer_margin_tpr',
                    'min_retailer_margin_f',
                    'min_retailer_margin_d',
                    'min_retailer_margin_fd',
                    'min_duration',
                    'max_duration',
                    'max_duration_event',
                    'min_gap_event',
                    'min_gap_block',
                    'min_discount_feature',
                    'min_discount_display',
                    'min_discount_feature_display',
                    'max_duration_block',
                    'min_duration_block'
                ];

                const instance = new AzureDatalakeClient();

                const hashString = str => crypto.createHash('md5').update(str).digest('hex');
                const supercategory = 'test';
                
                const result = await instance.ext.etl({
                    url: 'https://nusatradeadl.blob.core.windows.net/simulation-service/scenario-results/bnorris@enterrasolutions.com/89b015ae-f8f1-40a2-8e44-fcdc32945f42/BAKING/input/promo_group_constraints.csv.gz',
                    target: 'brentestetl',
                    overwrite:true,
                    transform: (acc, row) => {
                        try {

                            PROMO_CONSTRAINTS.forEach(constraintType => {
        
                                const hash = hashString(`${row.planning_account}${row.group_name}${constraintType}`);
                                let constraint = acc.find(row => row.uid === hash);
        
                                if(!constraint) {
                                    constraint = {
                                        partitionKey: 'global',
                                        rowKey: hash,
                                        uid: hash,
                                        supercategory,
                                        planning_account: row.planning_account,
                                        constraint_type: constraintType,
                                        constraint_value: row[constraintType],
                                        level: 'group_name',
                                        level_id: row.group_name,
                                        level_description: row.group_description,
                                        level_name: (row.group_name_name || row.group_name),
                                        hierarchy: [row.planning_account, row.group_name]
                                    }
                                    acc.push(constraint)
                                }
                            });
                            return acc;
                        }
                        catch( err ) {
                            console.log('<<<<<<<<<<<<<<>>>>>>>')
                        }
                    },
                    postFn: async data => {
                        return data.map(constraint => {
                            constraint.hierarchy = typeof constraint.hierarchy !== 'string' ? JSON.stringify(constraint.hierarchy) : constraint.hierarchy;
                            return constraint;
                        });
                    }
                });

            })
        });

    });

    describe(`Transform tests`, async () => {

        describe(`Loader.fromAzureDatalake`, () => {

            it(`Loader.fromAzureDataLake`, async () => {

                let errored;

                const resolved = await new Promise(async (resolve, reject) => {

                    pipeline(
                        await fromAzureDatalake({url: 'https://nusatradeadl.blob.core.windows.net/dev/test/reference_calendar.csv.gz'}),
                        CSVStreamToKeywordObjects(),
                        inspect(),
                        err => {
                            if(err) {
                                errored = true;
                                return reject(err);
                            }
                            else
                                resolve(true);
                        }
                    )
                });

                assert(resolved === true && errored === undefined, 'failed');

            });
        });

        describe(`Loader.toAzureDataLake`, () => {

            it(`Applies mutations to the mutation pipeline`, async () => {

                let errored;

                const resolved = await new Promise(async (resolve, reject) => {

                    pipeline(
                        await fromAzureDatalake({url: 'https://nusatradeadl.blob.core.windows.net/dev/test/reference_calendar.csv.gz'}),
                        CSVStreamToKeywordObjects(),
                        applyMutations({
                            pk: data => {
                                const {
                                    planning_account,
                                    group_name,
                                    start_date,
                                    promo_tactic
                                } = data;
                                return [planning_account, group_name, start_date, promo_tactic].join('|');
                            },
                            modifications: [
                                {
                                    "planning_account": "AWG KC - Combined",
                                    "start_date": "20210103",
                                    "promo_tactic": "EDLP",
                                    "group_name":"USSS2S208S208BK",
                                    "duration": "9999"
                                }
                            ]
                        }),
                        await keywordArrayToCSV({parserOptions:{delimiter:'|'}}),
                        await toAzureDatalake({url: 'https://nusatradeadl.blob.core.windows.net/dev/test/out_test.csv.gz'}),
                        err => {
                            if(err) {
                                errored = true;
                                return reject(err);
                            }
                            else
                                resolve(true);
                        }
                    )
                });

                assert(resolved === true && errored === undefined, 'failed');

            });
        });
        
        describe(`Loader.toAzureDataTables`, async () => {

            it.only(`loads some data to the tables`, async () => {

                let errored;
                const table = 'toAzureDataTablesTest';

                const resolved = await new Promise(async (resolve, reject) => {

                    pipeline(
                        await fromAzureDatalake({url: 'https://nusatradeadl.blob.core.windows.net/dev/test/reference_calendar_small.csv'}),
                        CSVStreamToKeywordObjects(),
                        applyAzureKeys({partitionKey: 'planning_account', rowKey: ['planning_account', 'group_name', 'start_date', 'promo_tactic']}),
                        await toAzureDataTables({table, overwrite:true}),
                        err => {
                            if(err) {
                                errored = true;
                                return reject(err);
                            }
                            else
                                resolve(true);
                        }
                    )
                });

                assert(resolved === true && errored === undefined, 'failed');

            });

        });

    });

    describe(`Single File Test`, () => {

        it(`Tests a reduce on a given file`, async () => {

        });

    });

});