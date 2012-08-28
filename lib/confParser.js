var fs = require('fs');
var iniparser = require('iniparser');

/**
* Given a js conf object, write it out to a file in conf format
* @param: {String} file The path of the output file.
* @param: {object} conf The conf object.
* @param: {function} callback The callback of the form f(err).
*/
module.exports.write = function write(file, conf, callback) {
        var confStr = '';
        for (var key in conf) {
                confStr += key + ' = ' + conf[key] + '\n';
        }

        fs.writeFile(file, confStr, callback);
};

/**
* Sets the value of a key in the conf.
* @param {object} conf The conf object.
* @param {String} key The configuration key.
* @param {String} value The configuration value.
*/
module.exports.set = function setValue(conf, key, value) {
        conf[key] = value;
};

/**
* Reads a .conf file into a conf object.
* @param {String} file The path to the file.
* @param {function} callback The callback in the form of f(err, conf).
*/
module.exports.read = function read(file, callback) {
        iniparser.parse(file, callback);
};
