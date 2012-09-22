#!/usr/bin/env node
// -*- mode: js -*-
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var carrier = require('carrier');
var stream = require('stream');
var util = require('util');

var my_carrier = carrier.carry(process.stdin);
process.stdin.resume();

// see: http://www.postgresql.org/docs/9.2/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
var PG_REGEX_TABLE = /^COPY ([\w]+) \(((?:[\"?\w\"?]+[\,]?[ ]?){1,})\) FROM stdin\;$/;

var PG_NULL = '\\N';
var PG_END = '\\.';

var schema = null;
my_carrier.on('line',  function(line) {
        if (!schema) {
                schema = PG_REGEX_TABLE.exec(line);
                if (schema) {
                        var name = schema[1];
                        var keys = schema[2].split(', ');
                        // strip out "" in certain keys that conflict with pg
                        // keywords
                        for (var i = 0; i < keys.length; i++) {
                                var key = keys[i];
                                if (key[0] === '"' &&
                                    key[key.length - 1] === '"') {
                                        keys[i] = key.substr(1, key.length - 2);
                                }
                        }
                        var table = {
                                name: name,
                                keys: keys
                        };
                        console.log(JSON.stringify(table));
                }
        } else if (line === PG_END) {
                process.exit(0);
        } else {
                var entry = {
                        entry: line.split('\t')
                };
                console.log(JSON.stringify(entry));
        }
});



