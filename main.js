import { deepStrictEqual } from "assert";
import { encode, decodeArrayStream } from "@msgpack/msgpack";
import { parse } from 'csv-parse';
import { stringify } from 'csv-stringify/sync';
import { Readable } from 'stream';
import blockIterator from 'block-iterator'


function streamToString(stream) {
    const chunks = [];
    return new Promise((resolve, reject) => {
        stream.on('data', (chunk) => { chunks.push(Buffer.from(chunk)) });
        stream.on('error', (err) => { console.log("error:"); console.log(err); reject(err) });
        stream.on('end', () => { console.log("resolving"); resolve(Buffer.concat(chunks).toString('utf8')) });
    })
}

class JSONParser {
    serialize = async (records) => {
        return JSON.stringify(records);
    }

    deserialize = async (stream) => {
        return JSON.parse(await streamToString(stream));
    }
}

class CSVParser {
    serialize = async (records) => {
        return stringify(records, { header: true });
    }

    deserialize = async (stream) => {
        const parser = stream.pipe(parse({
            from: 2 // 1 indexed
        }));
        const results = [];
        for await (const record of parser) {
            results.push({
                firstName: record[0],
                lastName: record[1],
                teamId: +record[2]
            });
        }
        return results;
    }
}

class MsgPackParser {
    serialize = async (records) => {
        return encode(records);
    }

    deserialize = async (stream) => {
        const results = [];
        // in an async function:
        for await (const item of decodeArrayStream(stream)) {
            results.push(item);
        }

        return results;
    }
}

const generateRecord = () => {
    return {
        firstName: "asd",
        lastName: "asd",
        teamId: 12345
    }
}

const generateRecords = (numberOfRecords) => {
    const records = [];

    for (let i = 0; i < numberOfRecords; i++) {
        records.push(generateRecord());
    }

    return records;
}

const verify = (expectedResults, actualResults) => {
    if (expectedResults.length !== actualResults.length) {
        throw new Error(`expected and actual results differ in length ${expectedResults.length} ${actualResults.length}`);
    }

    for (let i = 0; i < expectedResults.length; i++) {
        const expected = expectedResults[i];
        const actual = actualResults[i];
        deepStrictEqual(expected, actual);
    }
}

const main = async () => {
    const maxAttempts = 1;
    const numberOfRecords = 10000;

    // const parser = new CSVParser();
    // const parser = new JSONParser();
    const parser = new MsgPackParser();

    for (let i = 0; i < maxAttempts; i++) {
        const records = generateRecords(numberOfRecords);
        const serializedString = await parser.serialize(records);
        const newTestStream = Readable.from(blockIterator(serializedString, 64_000, { zeroPadding: false }));
        console.log(serializedString);
        const start = performance.now();
        const results = await parser.deserialize(newTestStream);
        const end = performance.now();
        console.log(`Execution time: ${end - start} ms`);
        // console.log(results);
        verify(records, results);
    }
}

main()
    .then(text => {
        console.log(text);
    })
    .catch(err => {
        console.log(err);
    });