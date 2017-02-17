const Q = require('q');

function genericArrayPromiseIterator(list, oneach, callback) {
    list = list.slice(0);

    function next() {
        var a = list.shift();
        if (!a) {
            return Q(null);
        }
        var p = oneach(a)
        if (p.then) {
            return p.then(callback).then(next);
        } else {
            return callback(p).then(next);
        }
    }
    return next();
}

module.exports = genericArrayPromiseIterator;