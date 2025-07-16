/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2025 Edgecast Cloud LLC.
 */

/*
 * @overview Prometheus metrics formatter for Manatee cluster health
 */

var assert = require('assert');

/*
 * Format cluster details as Prometheus metrics text
 *
 * @param {Object} clusterDetails - Result from loadClusterDetails()
 * @param {Object} config - Configuration object
 * @param {String} config.shardPath - Shard identifier
 * @returns {String} Prometheus formatted metrics
 */
function formatPrometheusMetrics(clusterDetails, config) {
    assert(clusterDetails && (typeof (clusterDetails) === 'object'),
        'clusterDetails must be an object');
    assert(config && (typeof (config) === 'object'),
        'config must be an object');
    assert((typeof (config.shardPath) === 'string'),
        'config.shardPath must be a string');

    var lines = [];
    var shard = config.shardPath;
    var peers = clusterDetails.pgs_peers || {};
    var peerIds = Object.keys(peers);

    if (peerIds.length === 0) {
        return '# No peers found\n';
    }

    // PostgreSQL connectivity metric header
    addMetricHelp(lines, 'manatee_peer_postgres_up',
        'PostgreSQL connectivity (1=up, 0=down)');
    addMetricType(lines, 'manatee_peer_postgres_up', 'gauge');

    // Add metric for each peer
    peerIds.forEach(function (peerId) {
        var peer = peers[peerId];
        var role = getPeerRole(peerId, clusterDetails);
        var labels = {
            shard: shard,
            peer: peerId,
            role: role
        };

        // PostgreSQL connectivity - absence of pgp_pgerr means healthy
        var pgUp = peer.pgp_pgerr ? 0 : 1;
        addMetric(lines, 'manatee_peer_postgres_up', pgUp, labels);
    });

    return lines.join('\n') + '\n';
}

/*
 * Determine the role of a peer in the cluster
 */
function getPeerRole(peerId, details) {
    if (peerId === details.pgs_primary) {
        return 'primary';
    }
    if (peerId === details.pgs_sync) {
        return 'sync';
    }
    if ((details.pgs_asyncs || []).indexOf(peerId) !== -1) {
        return 'async';
    }
    if ((details.pgs_deposed || []).indexOf(peerId) !== -1) {
        return 'deposed';
    }
    return 'unknown';
}

/*
 * Add a metric help comment
 */
function addMetricHelp(lines, name, description) {
    lines.push('# HELP ' + name + ' ' + description);
}

/*
 * Add a metric type comment
 */
function addMetricType(lines, name, type) {
    lines.push('# TYPE ' + name + ' ' + type);
}

/*
 * Add a metric line with labels and value
 */
function addMetric(lines, name, value, labels) {
    var labelStr = '';
    if (labels && Object.keys(labels).length > 0) {
        labelStr = '{' + Object.keys(labels)
            .map(function (key) {
                return key + '="' + escapeLabel(labels[key]) + '"';
            })
            .join(',') + '}';
    }

    lines.push(name + labelStr + ' ' + value);
}

/*
 * Escape label values for Prometheus format
 */
function escapeLabel(value) {
    var quote = '"';
    return String(value)
        .replace(/\\/g, '\\\\')  // Escape backslashes
        .replace(new RegExp(quote, 'g'), '\\' + quote)    // Escape quotes
        .replace(/\n/g, '\\n');  // Escape newlines
}

/*
 * Format an error response in Prometheus format
 */
function formatPrometheusError(err) {
    var lines = [];
    addMetricHelp(lines, 'manatee_scrape_error',
        'Error occurred during metrics collection');
    addMetricType(lines, 'manatee_scrape_error', 'gauge');
    addMetric(lines, 'manatee_scrape_error', 1,
        {error: err.message || 'unknown'});
    return lines.join('\n') + '\n';
}

module.exports = {
    formatPrometheusMetrics: formatPrometheusMetrics,
    formatPrometheusError: formatPrometheusError
};
