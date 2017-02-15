var fs = require('fs');
var Q = require('q');
var path = require('path');
var excludes = require('./file_excludes');
var hash = require('./hash');
var promiseLimit = require("./promise_limit");

var file_limit = promiseLimit(5);

function chunkFile(file, store) {
    var g_fd = null;
    return Q.ninvoke(fs, 'open', file.fullpath, 'r').then((fd) => {
        g_fd = fd;
        console.log("Opened: " + file.fullpath + " ");

        const size = 1048576 * 2 * 2; // 4M chunks
        var chunks = [];
        var chunkcount = Math.floor(file.stat.size / size) + 1;

        function nextChunk() {
            var buffer = Buffer.allocUnsafe(size);
            return Q.nfcall(fs.read, fd, buffer, 0, buffer.length, null).then((bytesread) => {
                bytesread = bytesread[0];
                buffer.length = bytesread;
                buffer = buffer.slice(0, bytesread);
                if (bytesread !== buffer.length) {
                    throw "BUFFER NOT EQUAL" + [bytesread, buffer.length].join(',');
                }
                if (bytesread === 0) {
                    console.log("Read all of file: " + file.fullpath);
                    return chunks
                } else {
                    return store.storeChunk(buffer).then(function(chunk_data) {
                        chunks.push(chunk_data);
                        console.log(file.name + " " + chunks.length + " of " + chunkcount + " " + chunk_data);
                        return nextChunk();
                    })
                }
            });
        }

        return nextChunk();

    }).finally(() => {
        if (g_fd) {
            console.log("Closing: " + file.fullpath);
            fs.closeSync(g_fd);
        }
    }).catch((error) => {
        console.log("Error reading: " + file.fullpath);
        console.log(typeof error);
        console.log(JSON.stringify(error));
        return null;
    })
}

function obj_equals(a, b) {
    for (var key in b) {
        if (a[key] !== b[key]) {
            return false;
        }
    }
    return true;
}

function processFile(file, store, stat_cache) {
    function doanyway() {
        return chunkFile(file, store).then((chunks) => {
            if (!chunks) {
                return null;
            }

            return stat_cache.set(file.fullpath, {
                stat: file.stat,
                chunks: chunks
            }).then(() => {
                return chunks;
            });
        });
    }

    // console.log("Processing file: " + file.fullpath);
    return stat_cache.get(file.fullpath).then((olddata) => {
        if (olddata && obj_equals(olddata.stat, file.stat)) {
            return store.hasAllChunks(olddata.chunks).then((hasall) => {
                if (hasall) {
                    return olddata.chunks;
                } else {
                    console.log("Weird: The store didn't have my chunks");
                    console.log(JSON.stringify(olddata.chunks));
                    return doanyway();
                }
            });
        } else {
            return doanyway();
        }
    });

}

function safeStat(fullpath) {
    // We use our own promise here because we might catch
    // and handle a stat error
    var deferred = Q.defer();

    Q.nfcall(fs.stat, fullpath).then((s) => {
        deferred.resolve(s);
    }).catch(function(error) {
        console.log("Stat error: " + error);
        console.log(typeof error);
        console.log(error);
        deferred.resolve(null);
    }).done();

    return deferred.promise;
}

function process(dirname, store, stat_cache) {
    // Clean up the path
    dirname = path.resolve(dirname);
    var dirobj = {
        path: dirname
    };
    console.log("Processing dir: " + dirname);

    return Q.ninvoke(fs, "readdir", dirname).then((dirs) => {
        // We want the files in sorted order
        dirs = dirs.splice(0).sort();
        // Start statting all of the files
        var stats = dirs.map((file) => {
            var fullpath = path.join(dirname, file);

            if (excludes.isGood(file) === false) {
                return null;
            }

            return safeStat(fullpath).then((stat) => {
                // safeStat might return null if the file was inaccessable
                if (!stat) {
                    return null;
                }
                var storestat = {
                    mode: stat.mode,
                    uid: stat.uid,
                    gid: stat.gid,
                    size: stat.size,
                    mtime: stat.mtime.getTime()
                };
                //storestat.mtime = parseInt(storestat.mtime / 1000);
                var info = {
                    fullpath: fullpath,
                    name: file,
                    isFile: stat.isFile(),
                    isDirectory: stat.isDirectory(),
                    stat: storestat
                };
                info.unique_id = hash.hash(JSON.stringify(info));
                return info;
            });

        });
        return (Q.all(stats));
    }).then((stats) => {
        // Put the results of stats into file and dir buckets
        var dirs = [];
        var files = [];
        stats.forEach((f) => {
            if (!f) {
                return;
            } else if (f.isDirectory) {
                dirs.push(f);
            } else if (f.isFile) {
                files.push(f);
            }
        });
        stats = null; // Not necessary, but nulling it out to prevent access

        // We keep directories in this array
        var stored_dirs = [];



        // we store all the files at once, but it is actually
        // limited through file_limit.
        function storeAllFiles() {
            return Q.all(
                files.map(function(f) {
                    return file_limit(function() {
                        return processFile(f, store, stat_cache).then((chunks) => {
                            // Return the single element representation of this entry
                            return chunks ? (f.name + "*" + chunks.join(',')) : null;
                        })
                    })
                })
            ).then((stored_files) => {
                // Filter out null files
                stored_files = stored_files.filter(function(n) {
                    return n !== null
                });

                return store.storeDirectory(dirname, stored_dirs, stored_files);
            });
        }


        function nextDir() {
            var dir = dirs.pop();
            if (!dir) {
                // Done with dirs, do files
                //return nextFile();
                return storeAllFiles();
            } else {
                return process(dir.fullpath, store, stat_cache).then((s) => {
                    if (s) {
                        // Store single element representation of this entry
                        stored_dirs.push(dir.name + "*" + s);
                    } else {
                        console.log("Weird, nothing returned for dir")
                    }
                    return nextDir();
                });
            }
        }
        return nextDir();
    })
}


module.exports = {
    process: process
};