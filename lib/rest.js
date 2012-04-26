var restify = require('restify');
var Logger = require('bunyan');

var BACKUPS = {};

var log = new Logger({
  name: 'scallop-test',
  src: true,
  level: 'trace'
});

function respond(req, res, next) {
  log.error('req', req.params);
  log.error('query', req.query);
  // do the backup
  // send response
  res.header('Location', '/backup/' + req.query.ip);
  res.status(201);
  res.send();
}

function getBackupStatus(req, res, next) {
  var ip = req.ip;
  res.send()
}

var server = restify.createServer({
  log: log
});
server.use(restify.queryParser());

server.post('/backup', respond);

server.listen(8080, function() {
  console.log('%s listening at %s', server.name, server.url);
});
