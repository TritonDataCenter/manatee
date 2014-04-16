/**
 * @copyright Copyright (c) 2013, Joyent, Inc. All rights reserved.
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

/*
 * globals
 */

var NAME = 'manatee-sitter';

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'info'),
    name: NAME,
    serializers: {
        err: bunyan.stdSerializers.err
    },
    // always turn source to true, manatee isn't in the data path
    src: true
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
                if (LOG.level() <= bunyan.DEBUG)
                    LOG = LOG.child({src: true});
                break;

            default:
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
    _config.zkCfg.log = LOG;
    _config.postgresMgrCfg.log = LOG;
    _config.postgresMgrCfg.zfsClientCfg.log = LOG;
    _config.postgresMgrCfg.snapShotterCfg.log = LOG;
    _config.postgresMgrCfg.syncStateCheckerCfg.log = LOG;
    _config.heartbeatServerCfg.log = LOG;
    _config.heartbeatClientCfg.log = LOG;

    LOG.info('starting manatee');
    Shard.start(_config);
    LOG.info('manatee started');
})();
