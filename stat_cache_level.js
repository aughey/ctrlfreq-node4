const fs = require('fs');
const Q = require('q');

var levelup = require('levelup')

function open() {
    var db = levelup("./cache");

    function close() {
        db.close();
    }

    function get(key) {
        return Q.ninvoke(db, 'get', key).then((val) => {
            return JSON.parse(val);
        }).catch(() => {
            return null;
        })
    }

    function set(key, value) {
        return Q.ninvoke(db, 'put', key, JSON.stringify(value));
    }

    return Q({
        get: get,
        set: set,
        close: close
    });
}

module.exports = {
    open: open,
}