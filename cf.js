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

    function split_file(f) {
        var star = f.indexOf("*");
        var name = f.slice(0, star);
        var chunks = f.slice(star + 1).split(',');
        return {
            name: name,
            chunks: chunks,
            f: f
        };
    }

    function genericArrayPromiseIterator(list, oneach, callback) {
    	list = list.slice(0);
        function next() {
        	var a = list.shift();
        	if(!a) {
        		return Q(null);
        	}
        	return oneach(a).then(callback).then(next);
        }
        return next();
    }

    function wrap_file(info) {
        info.eachChunk = function(cb) {
        	return genericArrayPromiseIterator(info.chunks,(c) => { return store.getChunk(c) },cb);
        }
        return info;
    }

    function wrap_dir(prefix, root) {
        return store.getDir(root).then((d) => {
            return {
                eachFile: function(cb) {
                    var files = d.files.slice(0);

                    function next() {
                        var d = files.shift();
                        if (!d) {
                            return Q(null);
                        }
                        d = split_file(d);
                        d.root = root;
                        d.fullpath = path.join(prefix,d.name);
                        return cb(wrap_file(d)).then(next);
                    }

                    return next();
                },
                eachDir: function(cb) {
                    var dirs = d.dirs.slice(0);

                    function next() {
                        var d = dirs.shift();
                        if (!d) {
                            return Q(null);
                        }
                        d = split_file(d);
                        return wrap_dir(path.join(prefix, d.name), d.chunks[0]).then((res) => {
                            return cb(res).then(next);
                        });
                    }
                    return next();
                },
                name: prefix,
                id: root
            }
        });

    }

    function getLastBackup(dirname) {
        return store.getLastBackup(dirname).then((root) => {
            return wrap_dir(dirname, root);
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

            },
            getLastBackup: getLastBackup
        }
    });
}

module.exports = {
    open: open
}
