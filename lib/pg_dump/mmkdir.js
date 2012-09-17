#!/usr/bin/env node
// -*- mode: js -*-
// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var path = require('path');
var url = require('url');

var bunyan = require('bunyan');
var getopt = require('posix-getopt');
var restify = require('restify');
var uuid = require('node-uuid');

var manta = require('../lib');



///--- Globals

var LOG = bunyan.createLogger({
        name: path.basename(process.argv[1]),
        level: (process.env.LOG_LEVEL || 'info'),
        stream: process.stderr,
        serializers: restify.bunyan.serializers
});



///--- Functions

function ifError(err) {
        if (err) {
                console.error(err.toString());
                process.exit(1);
        }
}


function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('vh:k:u:',
                                            process.argv);
        var tmp;

        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'a':
                        opts.user = option.optarg;
                        break;

                case 'h':
                        opts.host = option.optarg;
                        break;

                case 'k':
                        opts.keyId = option.optarg;
                        break;

                case 'u':
                        opts.url = url.parse(option.optarg).href;
                        break;

                case 'v':
                        // Allows us to set -vvv -> this little hackery
                        // just ensures that we're never < TRACE
                        LOG.level(Math.max(bunyan.TRACE, (LOG.level() - 10)));
                        if (LOG.level() <= bunyan.DEBUG)
                                LOG = LOG.child({src: true});
                        break;

                default:
                        process.exit(1);
                        break;
                }

        }

        if (!opts.url && !process.env.MANTA_URL)
                usage('url is a required argument');

        if (!opts.user && !process.env.MANTA_USER)
                usage('account is a required argument');

        if (!opts.keyId && !process.env.MANTA_KEY_ID)
                usage('keyId is a required argument');

        if (parser.optind() >= process.argv.length)
                usage('missing required argument: "path"');

        opts.keyId = opts.keyId || process.env.MANTA_KEY_ID;
        opts.path = path.normalize(process.argv[parser.optind()]);
        opts.url = opts.url || process.env.MANTA_URL;
        opts.user = opts.user || process.env.MANTA_USER;
        return (opts);
}


function usage(msg) {
        if (msg)
                console.error(msg);

        var str = 'usage: ' + path.basename(process.argv[1]);
        str += '[-v] [-a account] [-u url] [-k keyId] path';
        console.error(str);
        process.exit(1);
}



///--- Mainline
// Leading _ to avoid scope conflicts in functions

var _options = parseOptions();
var _client = manta.createClient({
        connectTimeout: 1000,
        log: LOG,
        retry: true,
        sign: manta.sshAgentSigner({
                keyId: _options.keyId,
                log: LOG,
                user: _options.user
        }),
        url: _options.url,
        user: /^\/.*\/stor/.test(_options.path) ? undefined : _options.user,
        header: { 'x-marlin': 'true' }
});

_client.mkdir(_options.path, function (err) {
        ifError(err);

        process.exit(0);
});
