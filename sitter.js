/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 *
 */
var assert = require('assert-plus');
var bunyan = require('bunyan');
var extend = require('xtend');
var fs = require('fs');
var getopt = require('posix-getopt');
var Shard = require('./lib/shard');
var StatusServer = require('./lib/statusServer');

/*
 * globals
 */

var NAME = 'manatee-sitter';

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'info'),
    name: NAME,
    serializers: {
        err: bunyan.stdSerializers.err
    }
});

var LOG_LEVEL_OVERRIDE = false;

/*
 * private functions
 */

function parseOptions() {
    var option;
    var opts = {};
    var parser = new getopt.BasicParser('vf:(file)', process.argv);

    while ((option = parser.getopt()) !== undefined) {
        switch (option.option) {
            case 'f':
                opts.file = option.optarg;
                break;

            case 'v':
                // Allows us to set -vvv -> this little hackery
                // just ensures that we're never < TRACE
                LOG_LEVEL_OVERRIDE = true;
                LOG.level(Math.max(bunyan.TRACE, (LOG.level() - 10)));
                break;

            default:
                LOG.fatal('Unsupported option: ', option.option);
                process.exit(1);
                break;
        }
    }

    return (opts);
}

function readConfig(options) {
    assert.object(options);

    var cfg;

    try {
        cfg = JSON.parse(fs.readFileSync(options.file, 'utf8'));
    } catch (e) {
        LOG.fatal({
            err: e,
            file: options.file
        }, 'Unable to read/parse configuration file');
        process.exit(1);
    }

    return (extend({}, cfg, options));
}

/*
 * mainline
 */
(function main() {
    var _config;
    var _options = parseOptions();

    LOG.debug({options: _options}, 'command line options parsed');
    _config = readConfig(_options);
    LOG.debug({config: _config}, 'configuration loaded');

    if (_config.logLevel && !LOG_LEVEL_OVERRIDE) {
        if (bunyan.resolveLevel(_config.logLevel)) {
            LOG.level(_config.logLevel);
        }
    }


    // set loggers of the sub components
    _config.log = LOG;
    _config.postgresMgrCfg.log = LOG;
    _config.postgresMgrCfg.zfsClientCfg.log = LOG;
    _config.postgresMgrCfg.snapShotterCfg.log = LOG;

    LOG.info('starting manatee');
    var shard = Shard.start(_config);
    StatusServer.start({
        log: LOG,
        port: _config.postgresPort + 1,
        shard: shard
    });
    LOG.info('manatee started');

    // The smf manifest indicates sending a SIGINT (2) on disable
    /**
     * The idea here was to only remove the ZK node before shutdown.  I was
     * trying to do that via an SMF signal, but, unfortunately, I realized too
     * late (ie during testing) that since postgres is a child process of this,
     * it is part of the same contract and will get the SIGINT directly from
     * SMF.  So this will have to wait until the postgres process management
     * portions of Manatee are reworked.
     *
     * Also note MANATEE-188 which says that postgres *must* be killed without
     * writing a shutdown checkpoint, so it currently needs a SIGKILL
     * independent of this SIGINT.
     *
     * process.on('SIGINT', function () {
     *     LOG.info('Sitter.main: got SIGINT');
     *     if (!shard) {
     *         process.exit();
     *     }
     *     shard.shutdown(function (err) {
     *         LOG.info({err: err}, 'Sitter.main: done shutdown procedures');
     *         if (err) {
     *             process.abort();
     *         }
     *         process.exit();
     *     });
     * });
     */
})();
