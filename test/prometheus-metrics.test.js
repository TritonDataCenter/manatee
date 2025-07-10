/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2025 Edgecast Cloud LLC.
 */

/*
 * Test the Prometheus metrics formatter
 * Phase 1: Test only manatee_peer_postgres_up metric
 */

// Simple assert replacement
function assert(condition, message) {
    if (!condition) {
        throw new Error(message || 'Assertion failed');
    }
}

assert.ok = assert;
var prometheusFormatter = require('../lib/prometheusFormatter');

// Sample cluster details data (similar to what loadClusterDetails returns)
// Phase 1: Minimal data needed for postgres_up metric testing
var sampleClusterDetails = {
    pgs_peers: {
        '10.1.0.100:5432:12345': {
            // No pgp_pgerr means PostgreSQL is healthy
        },
        '10.1.0.101:5432:12345': {
            // No pgp_pgerr means PostgreSQL is healthy
        },
        '10.1.0.102:5432:12345': {
            pgp_pgerr: new Error('connection timeout')  // PostgreSQL is down
        }
    },
    pgs_primary: '10.1.0.100:5432:12345',
    pgs_sync: '10.1.0.101:5432:12345',
    pgs_asyncs: ['10.1.0.102:5432:12345']
};

var sampleConfig = {
    shardPath: '1.moray.emy-10.example.com'
};

function testBasicMetricsFormatting() {
    console.log('Testing basic metrics formatting');

    var metrics = prometheusFormatter.formatPrometheusMetrics(
        sampleClusterDetails, sampleConfig);

    console.log('Generated metrics:');
    console.log(metrics);

    // Basic validation - Phase 1: Only postgres_up metric
    assert.ok(metrics.indexOf('# HELP') !== -1, 'Should include help comments');
    assert.ok(metrics.indexOf('# TYPE') !== -1, 'Should include type comments');
    assert.ok(metrics.indexOf('manatee_peer_postgres_up') !== -1,
        'Should include postgres health metric');
    assert.ok(metrics.indexOf('shard="1.moray.emy-10.example.com"') !== -1,
        'Should include shard label');

    // Phase 1: Should NOT include other metrics
    assert.ok(metrics.indexOf('manatee_cluster_peers_total') === -1,
        'Should NOT include cluster metrics in Phase 1');
    assert.ok(metrics.indexOf('manatee_peer_replication_lag') === -1,
        'Should NOT include lag metrics in Phase 1');

    console.log('Basic formatting test passed');
}

function testErrorFormatting() {
    console.log('Testing error formatting');

    var testError = new Error('ZooKeeper connection failed');
    var errorMetrics = prometheusFormatter.formatPrometheusError(testError);

    console.log('Generated error metrics:');
    console.log(errorMetrics);

    assert.ok(errorMetrics.indexOf('manatee_scrape_error') !== -1,
        'Should include error metric');
    assert.ok(errorMetrics.indexOf('ZooKeeper connection failed') !== -1,
        'Should include error message');

    console.log('Error formatting test passed');
}

function testPostgresUpMetricValues() {
    console.log('Testing PostgreSQL up metric values');

    var metrics = prometheusFormatter.formatPrometheusMetrics(
        sampleClusterDetails, sampleConfig);

    // Phase 1: Test only postgres_up metric values
    assert.ok(metrics.indexOf(
        'peer="10.1.0.100:5432:12345",role="primary"} 1') !== -1,
        'Primary should be up (1)');
    assert.ok(metrics.indexOf(
        'peer="10.1.0.101:5432:12345",role="sync"} 1') !== -1,
        'Sync should be up (1)');
    assert.ok(metrics.indexOf(
        'peer="10.1.0.102:5432:12345",role="async"} 0') !== -1,
        'Async peer should be down (0)');

    console.log('PostgreSQL up metric values test passed');
}

function testPrometheusCompliance() {
    console.log('Testing Prometheus format compliance');

    var metrics = prometheusFormatter.formatPrometheusMetrics(
        sampleClusterDetails, sampleConfig);
    var lines = metrics.split('\n');

    // Check format compliance
    lines.forEach(function (line, index) {
        if (line.trim() === '') {
            return; // Skip empty lines
        }

        if (line.indexOf('# HELP ') === 0) {
            assert.ok(line.length > 7,
                'Help lines should have content after "# HELP "');
        } else if (line.indexOf('# TYPE ') === 0) {
            assert.ok(line.indexOf(' gauge') !== -1,
                'Type lines should specify gauge type');
        } else {
            // Metric lines should have format: metric_name{labels} value
            assert.ok(line.indexOf('{') !== -1 && line.indexOf('}') !== -1,
                'Metric line should have labels: ' + line);
            assert.ok(/\} \d+(\.\d+)?$/.test(line),
                'Metric line should end with numeric value: ' + line);
        }
    });

    console.log('Prometheus format compliance test passed');
}

function runAllTests() {
    console.log('Running Prometheus Metrics Tests');

    try {
        testBasicMetricsFormatting();
        testErrorFormatting();
        testPostgresUpMetricValues();
        testPrometheusCompliance();

        console.log('All tests passed');

    } catch (err) {
        console.error('Test failed:', err.message);
        console.error(err.stack);
        process.exit(1);
    }
}

// Run tests if called directly
if (require.main === module) {
    runAllTests();
}

module.exports = {
    runAllTests: runAllTests,
    sampleClusterDetails: sampleClusterDetails,
    sampleConfig: sampleConfig
};
