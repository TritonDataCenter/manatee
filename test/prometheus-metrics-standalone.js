/*
 * Standalone test for Prometheus metrics formatter
 * Phase 1: Tests only manatee_peer_postgres_up metric
 * Does not require npm dependencies
 */

// Simple assert replacement
function assert(condition, message) {
    if (!condition) {
        throw new Error(message || 'Assertion failed');
    }
}

// Load our formatter
var prometheusFormatter = require('../lib/prometheusFormatter');

// Sample cluster details data
// Phase 1: Minimal data for postgres_up metric testing
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

console.log('Testing Prometheus Metrics Formatter');

try {
    // Test basic formatting
    var metrics = prometheusFormatter.formatPrometheusMetrics(
        sampleClusterDetails, sampleConfig);

    console.log('Generated Prometheus metrics:');
    console.log(metrics);

    // Phase 1: Basic validations for postgres_up metric only
    assert(metrics.indexOf('# HELP') !== -1, 'Should include help comments');
    assert(metrics.indexOf('# TYPE') !== -1, 'Should include type comments');
    assert(metrics.indexOf('manatee_peer_postgres_up') !== -1,
        'Should include postgres health');
    assert(metrics.indexOf('shard="1.moray.emy-10.example.com"') !== -1,
        'Should include shard label');

    // Phase 1: Should NOT include cluster metrics
    assert(metrics.indexOf('manatee_cluster_peers_total') === -1,
        'Should NOT include cluster metrics in Phase 1');
    assert(metrics.indexOf('manatee_cluster_generation') === -1,
        'Should NOT include generation in Phase 1');

    // Test postgres_up specific values
    assert(metrics.indexOf(
        'peer="10.1.0.100:5432:12345",role="primary"} 1') !== -1,
        'Primary should be up (1)');
    assert(metrics.indexOf(
        'peer="10.1.0.102:5432:12345",role="async"} 0') !== -1,
        'Async should be down (0)');

    console.log('Basic metrics formatting: PASSED');

    // Test error formatting
    var testError = new Error('ZooKeeper connection failed');
    var errorMetrics = prometheusFormatter.formatPrometheusError(testError);

    console.log('Generated error metrics:');
    console.log(errorMetrics);

    assert(errorMetrics.indexOf('manatee_scrape_error') !== -1,
        'Should include error metric');
    assert(errorMetrics.indexOf('ZooKeeper connection failed') !== -1,
        'Should include error message');

    console.log('Error formatting: PASSED');

    console.log('All tests PASSED');

} catch (err) {
    console.error('Test FAILED:', err.message);
    process.exit(1);
}
