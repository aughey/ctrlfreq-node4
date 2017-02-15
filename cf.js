var Q = require('q');
const mongo_store = require("./mongo_store");
var stat_cache = require("./stat_cache_level");
var process_dir = require("./process_dir");
const path = require('path')

function open() {
    var cache = null;
    var store = null;

    function process(dirname) {
        dirname = path.resolve(dirname);

        return process_dir.process(dirname, store, cache).then((res) => {
            return store.storeBackup(dirname, res);
        });
    }

    return stat_cache.open().then((c) => {
        cache = c;
        return mongo_store.open();
    }).then((s) => {
        store = s;

        return {
            process: process,
            DELETE: function() {
            	return store.DELETE();
            },
            close: function() {
                return Q.all([
                    store.close(),
                    cache.close(),
                ]);
                store = null;
                cache = null;

            }
        }
    });
}

module.exports = {
    open: open
}