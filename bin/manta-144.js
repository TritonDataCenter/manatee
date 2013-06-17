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

        client.query('select * from manta where _vnode is null limit 100 for update', function(err, result) {
            assert.ifError(err);
            var rows = result.rows;
            var finishedRows = 0;
            if (rows.length === 0) {
                console.log('no more rows to backfill exiting');
                process.exit();
            }

            for (var i = 0; i < rows.length; i++) {
                var row = rows[i];
                vnode = JSON.parse(row._value).vnode;
                if (!vnode) {
                    throw new Error('vnode dne');
                }
                client.query('update manta set _vnode = ' + vnode + ' where _id = ' + row._id, function(err, result) {
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

var pgClient = new Client('tcp://postgres@localhost:5432/moray')
function bfcb(err) {
    if (err) {
        throw new Error(err);
    }

    backfill(pgClient, bfcb);
}

pgClient.connect(function(err) {
    if (err) {
        throw new Error(err);
    }
    console.log('connected');

    backfill(pgClient, bfcb);
});
