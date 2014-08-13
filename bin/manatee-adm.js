#!/usr/bin/env node
// -*- mode: js -*-

var fs = require('fs');
var util = require('util');

var adm = require('../lib/adm');
var cmdln = require('cmdln');


function ManateeAdm() {
    cmdln.Cmdln.call(this, {
        name: 'manatee-adm',
        desc: 'Inspect and administer Manatee'
    });
}
util.inherits(ManateeAdm, cmdln.Cmdln);

/**
 * Display the current state of the manatee shard(s).
 */
ManateeAdm.prototype.do_status = function do_status(subcmd, opts, args, cb) {
    var self = this;
    if (opts.help) {
        self.do_help('help', {}, [subcmd], cb);
    }

    if (!opts.zk) {
        self.do_help('help', {}, [subcmd], cb);
    }

    adm.status(opts, function (err, status) {
        if (err) {
            return cb(err);
        } else {
            console.log(JSON.stringify(status));
            return cb();
        }
    });
};
ManateeAdm.prototype.do_status.options = [ {
    names: ['help', 'h'],
    type: 'bool',
    help: 'Show this help'
}, {
    names: ['shard', 's'],
    type: 'string',
    helpArg: 'SHARD',
    help: 'The manatee shard to stat. If empty status will show all shards'
}, {
    names: ['zk', 'z'],
    type: 'string',
    helpArg: 'ZOOKEEPER_URL',
    help: 'The zookeeper connection string. e.g. 127.0.0.1:2181'
}];
ManateeAdm.prototype.do_status.help = (
    'Show status of a manatee shard. \n' +
    '\n' +
    'Usage:\n' +
    '    {{name}} status [OPTIONS]\n' +
    '\n' +
    '{{options}}'
);

/**
 * Clear a manatee shard out of safe mode.
 */
ManateeAdm.prototype.do_clear = function do_clear(subcmd, opts, args, cb) {
    var self = this;
    if (opts.help) {
        self.do_help('help', {}, [subcmd], cb);
    }

    if (!opts.shard) {
        self.do_help('help', {}, [subcmd], cb);
    }

    adm.clear(opts, cb);
};
ManateeAdm.prototype.do_clear.options = [ {
    names: ['help', 'h'],
    type: 'bool',
    help: 'Show this help'
}, {
    names: ['shard', 's'],
    type: 'string',
    helpArg: 'SHARD',
    help: 'The manatee shard to clear'
}, {
    names: ['zk', 'z'],
    type: 'string',
    helpArg: 'ZOOKEEPER_URL',
    help: 'The zookeeper connection string. e.g. 127.0.0.1:2181',
    default: '127.0.0.1:2181'
}];
ManateeAdm.prototype.do_clear.help = (
    'Clear a shard out of safe mode. \n' +
    '\n' +
    'Usage:\n' +
    '    {{name}} status [OPTIONS]\n' +
    '\n' +
    '{{options}}'
);

/**
 * Rebuild a manatee peer in a shard.
 */
ManateeAdm.prototype.do_rebuild = function do_rebuild(subcmd, opts, args, cb) {
    var self = this;
    if (opts.help) {
        self.do_help('help', {}, [subcmd], cb);
    }
    if (!opts.config) {
        self.do_help('help', {}, [subcmd], cb);
    }

    var cfg;

    try {
        cfg = JSON.parse(fs.readFileSync(opts.config, 'utf8'));
        opts.config = cfg;
    } catch (e) {
        return cb(e);
    }
    adm.rebuild(opts, cb);
};
ManateeAdm.prototype.do_rebuild.options = [ {
    names: ['help', 'h'],
    type: 'bool',
    help: 'Show this help'
}, {
    names: ['config', 'c'],
    type: 'string',
    helpArg: 'CONFIG',
    help: 'The path to the manatee sitter config to list'
}];
ManateeAdm.prototype.do_rebuild.help = (
    'Rebuild a manatee zone. \n' +
    '\n' +
    'Usage:\n' +
    '    {{name}} status [OPTIONS]\n' +
    '\n' +
    '{{options}}'
);

/**
 * Promote a manatee peer to the primary of the shard.
 */
ManateeAdm.prototype.do_promote = function do_promote(subcmd, opts, args, cb) {
    var self = this;
    if (opts.help) {
        self.do_help('help', {}, [subcmd], cb);
    }
    if (!opts.config) {
        self.do_help('help', {}, [subcmd], cb);
    }

    var cfg;

    try {
        cfg = JSON.parse(fs.readFileSync(opts.config, 'utf8'));
        opts.config = cfg;
    } catch (e) {
        return cb(e);
    }
    adm.promote(opts, cb);
};
ManateeAdm.prototype.do_promote.options = [ {
    names: ['help', 'h'],
    type: 'bool',
    help: 'Show this help'
}, {
    names: ['config', 'c'],
    type: 'string',
    helpArg: 'CONFIG',
    help: 'The path to the manatee sitter config to list'
}];
ManateeAdm.prototype.do_promote.help = (
    'Promote a manatee peer to the primary of the shard. \n' +
    '\n' +
    'Usage:\n' +
    '    {{name}} status [OPTIONS]\n' +
    '\n' +
    '{{options}}'
);

cmdln.main(ManateeAdm);
