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

describe(`Datalake client tests`, function() {

    let instance:AzureDatalakeClient;
    let validURL = `https://nusatradeadl.blob.core.windows.net/dev/working/BEVERAGE%20RTD/calendar_constraints.csv`;
    let validURLNotExists = `https://nusatradeadl.blob.core.windows.net/dev/working/BEVERAGE%20RTD/nofilehere.csv`;

    describe(`Setup`, () => {

        describe(`Environment`, () => {

            it(`Has AZURE_TENANT_ID`, () => Boolean(process.env.AZURE_TENANT_ID));
            it(`Has AZURE_CLIENT_ID`, () => Boolean(process.env.AZURE_CLIENT_ID));
            it(`Has AZURE_CLIENT_SECRET`, () => Boolean(process.env.AZURE_CLIENT_SECRET));

        })

        describe(`Instantiation`, () => {

            it(`Instances AzureDatalakeClient`, () => {

                instance = new  AzureDatalakeClient();
            });

        });

    });

    describe(`Method Tests`, () => {

        describe(`_parseURL`, () => {

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

            it(`invokes getCredential`, () => {

                const result = instance.getCredential()
                assert(result instanceof DefaultAzureCredential, 'failed');
            })

        });

        describe(`getServiceClient`, () => {

            const testURL = `https://nusatradeadl.blob.core.windows.net/dev/working/BEVERAGE%20RTD/calendar_constraints.csv`;
            let result:DataLakeServiceClient;

            it(`invokes the method`, () => {

                result = instance.getServiceClient({
                    url: testURL
                });
            });

            it(`Returned the ServiceClient`, () => {

                assert(result instanceof DataLakeServiceClient, 'failed')
            });

            it(`Cached the client in the instance, keyed to its hosturl`, async () => {

                const { hostURL } = instance._parseURL(testURL)
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

            let result:DataLakeFileClient;

            it(`invokes the method`, () => {

                result = instance.getFileClient({url: validURL});
                console.log('---')
            });


        });

        describe(`exists`, () => {

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

        describe(`download`, () => {

            it(`Invokes download upon a valid URL`, async () => {
                const result = await instance.download({url: validURL});
                assert(typeof result === 'string' && result.length > 1, 'failed');
            });
        });

        describe(`save`, () => {

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

});