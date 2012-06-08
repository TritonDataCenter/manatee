var util = require('util');



///--- Globals

var sprintf = util.format;



///--- API

function assertArgument(name, type, arg) {
        if (typeof (arg) !== type)
                throw new TypeError(sprintf('%s (%s) is required', name, type));
}


module.exports.assertFunction = function assertFunction(name, arg) {
        assertArgument(name, 'function', arg);
};


module.exports.assertNumber = function assertNumber(name, arg) {
        assertArgument(name, 'number', arg);
};


module.exports.assertObject = function assertObject(name, arg) {
        assertArgument(name, 'object', arg);
};


module.exports.assertString = function assertString(name, arg) {
        assertArgument(name, 'string', arg);
};
