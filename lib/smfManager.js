var restify = require('restify');

function SmfManager(options) {
  this.log = options.log;

  this.server = restify.createServer();
}
