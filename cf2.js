var Q = require('q');
//var process_dir = require("./process_dir");
const path = require('path');
var promiseLimit = require("./promise_limit");

var file_limit = promiseLimit(5);

function joinNameChunks(name, keys) {
    return [name, keys.join(',')].join('*');
}

function processSubDirs(d, store) {
    var subdir_info = [];
    return d.forEachDirectory((subdir) => {
        return processDir(subdir, store).then((key) => {
            if (key) {
                subdir_info.push(joinNameChunks(subdir.name, [key]));
            }
        });
    }).then(() => {
        return subdir_info;
    })
}

function readAndStoreFile(file, store) {
    var chunk_count = file.chunkCount();
    // This foreach will accumulate our returns and return them back to us in a list
    var count = 1;
    return file.forEachChunk((chunk) => {
        console.log(file.name + ": Writing chunk " + count + " of " + chunk_count);
        count++;
        return store.storeChunk(chunk)
    }).then((chunks) => {
        console.log(chunks);
        return file.rememberChunks(chunks).then(() => {
            return chunks;
        })
    })
}

function processFiles(d, store) {
    var file_info = [];
    return d.forEachFile((file) => {
        // See if we already know the chunks of this file
      //  console.log(file.fullpath);
        return file.getCachedChunks().then((chunks) => {
            if (!chunks) {
                console.log("Cached chunks not available");
                return readAndStoreFile(file, store);
            } else {
                return store.hasAllChunks(chunks).then((has_all) => {
                    if (has_all) {
                        return chunks;
                    } else {
                        console.log("Unusual: Store doesn't have all of my chunks for " + file.fullpath);
                        console.log(chunks);
                        return readAndStoreFile(file, store);
                    }
                })
            }
        }).then((chunks) => {
            if(chunks) {
                file_info.push(joinNameChunks(file.name,chunks));
            }
        })
    }).then(() => {
        return file_info;
    });
}

function processDir(dir, store) {
    console.log(dir.fullpath);
    // We use the nested promise structure because we want to keep the structure.
    return dir.open().then(function(d) {
        // open could fail, so we return quietly
        if (!d) {
            return null;
        }
        return processSubDirs(d, store).then((subdir_info) => {
            return processFiles(d, store).then((file_info) => {
                return store.storeDirectory(dir.fullpath, subdir_info, file_info);
            });
        });
    })
}

// This is what we are using on the "stock system"
function commonOpen() {
    var cache = null;
    var store = null;
    const mongo_store = require("./mongo_store");
    var stat_cache = require("./stat_cache_level");

    // Helper function to do filesystem work
    function process(dirname) {
        dirname = path.resolve(dirname);

        var file_system_reader = require("./file_system_reader");

        var dir = file_system_reader.wrap_dir({fullpath: dirname, name: "ROOT"}, cache);

        return processDir(dir, store).then((res) => {
            return store.storeBackup(dirname, res);
        })

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

    // function split_file(f) {
    //     var star = f.indexOf("*");
    //     var name = f.slice(0, star);
    //     var chunks = f.slice(star + 1).split(',');
    //     return {
    //         name: name,
    //         chunks: chunks,
    //         f: f
    //     };
    // }

    // function genericArrayPromiseIterator(list, oneach, callback) {
    //     list = list.slice(0);

    //     function next() {
    //         var a = list.shift();
    //         if (!a) {
    //             return Q(null);
    //         }
    //         return oneach(a).then(callback).then(next);
    //     }
    //     return next();
    // }

    // function wrap_file(info) {
    //     info.eachChunk = function(cb) {
    //         return genericArrayPromiseIterator(info.chunks, (c) => {
    //             return store.getChunk(c)
    //         }, cb);
    //     }
    //     return info;
    // }

    // function wrap_dir(prefix, root) {
    //     return store.getDir(root).then((d) => {
    //         return {
    //             eachFile: function(cb) {
    //                 var files = d.files.slice(0);

    //                 function next() {
    //                     var d = files.shift();
    //                     if (!d) {
    //                         return Q(null);
    //                     }
    //                     d = split_file(d);
    //                     d.root = root;
    //                     d.fullpath = path.join(prefix, d.name);
    //                     return cb(wrap_file(d)).then(next);
    //                 }

    //                 return next();
    //             },
    //             eachDir: function(cb) {
    //                 var dirs = d.dirs.slice(0);

    //                 function next() {
    //                     var d = dirs.shift();
    //                     if (!d) {
    //                         return Q(null);
    //                     }
    //                     d = split_file(d);
    //                     return wrap_dir(path.join(prefix, d.name), d.chunks[0]).then((res) => {
    //                         return cb(res).then(next);
    //                     });
    //                 }
    //                 return next();
    //             },
    //             name: prefix,
    //             id: root
    //         }
    //     });

    // }

    // function getLastBackup(dirname) {
    //     return store.getLastBackup(dirname).then((root) => {
    //         return wrap_dir(dirname, root);
    //     });
    // }

    // return stat_cache.open().then((c) => {
    //     cache = c;
    //     return mongo_store.open();
    // }).then((s) => {
    //     store = s;

    //     return {
    //         process: process,
    //         DELETE: function() {
    //             return store.DELETE();
    //         },
    //         close: function() {
    //             return Q.all([
    //                 store.close(),
    //                 cache.close(),
    //             ]);
    //             store = null;
    //             cache = null;

    //         },
    //         getLastBackup: getLastBackup
    //     }
    // });
}

module.exports = {
    commonOpen: commonOpen,
    processDir,
    processDir
}