#!/usr/bin/env node
// -*- mode: js -*-
// Backfill vnode column

var pg = require('pg');
var Client = pg.Client;
var assert = require('assert');

function backfill(client, cb) {
    console.log('begin');

    client.query('begin', function(err) {
        assert.ifError(err);

        client.query('select * from manta where _key like \'%\\%%\' for update', function(err, result) {
            assert.ifError(err);
            var rows = result.rows;
            //console.log(rows);
            var finishedRows = 0;
            if (rows.length === 0) {
                console.log('no more rows to backfill exiting');
                process.exit();
            }

            for (var i = 0; i < rows.length; i++) {
                var row = rows[i];
                var key = row._key;
                var value = JSON.parse(row._value);
                // date < 1pm 7/8/2013
                if (row.type === 'directory' || parseInt(row._mtime, 10) > 1373313600000) {
                    console.log('skipping key %s since it\'s a dir or created after deployment', key);
                    if (++finishedRows === rows.length) {
                        console.log('committing ' + rows.length + ' rows');
                        client.query('commit', cb);
                        break;
                    } else {
                        continue;
                    }
                }
                assert(key, 'no key!');
                var keyArray = key.split('/');
                assert(keyArray.length, 'no slashes in key!');

                var name = keyArray[keyArray.length - 1];
                try {
                    keyArray[keyArray.length - 1] = decodeURIComponent(name);
                    if (name === keyArray[keyArray.length -1]) {
                        console.log('skipping key %s name %s not url encoded, skipping segment', key, name);
                        if (++finishedRows === rows.length) {
                            console.log('committing ' + rows.length + ' rows');
                            client.query('commit', cb);
                            break;
                        } else {
                            continue;
                        }
                    }
                } catch (e) {
                    console.log('skipping key %s name %s not url encoded, skipping segment', key, name);
                    if (++finishedRows === rows.length) {
                        console.log('committing ' + rows.length + ' rows');
                        client.query('commit', cb);
                        break;
                    } else {
                        continue;
                    }
                }

                var decodedKey = keyArray.join('/');
                value.key = decodedKey;
                var decodedName = decodeURIComponent(value.name);
                value.name = decodedName;
                var decodedValue = JSON.stringify(value);
                console.log('updating key %s to decoded key %s', key, decodedKey);
                console.log('updating value %s to decoded value %s', JSON.stringify(value), decodedValue);
                var uq = 'update manta set _key = \'' + decodedKey +
                    '\'' + ', name = ' + '\'' + decodedName + '\'' +
                     ', _value = ' + '\'' + decodedValue + '\'' +
                    ' where _id = ' + row._id;

                client.query(uq, function(err, result) {
                    assert.ifError(err);
                    if (++finishedRows === rows.length) {
                        console.log('committing ' + rows.length + ' rows');
                        client.query('commit', cb);
                    }
                });
            }
        });
    });
}

//var pgClient = new Client('tcp://postgres@10.99.99.43:5432/moray')
var pgClient = new Client('tcp://postgres@localhost:5432/moray')
function bfcb(err) {
    if (err) {
        throw new Error(err);
    }

    process.exit();
}

pgClient.connect(function(err) {
    if (err) {
        throw new Error(err);
    }
    console.log('connected');

    backfill(pgClient, bfcb);
});
