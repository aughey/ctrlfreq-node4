'use strict'
var Q = require('q');
var fs = require('fs');
var path = require('path');
var excludes = require('./file_excludes');


function safeStat(fullpath, filename) {
    return Q.nfcall(fs.stat, fullpath).then((stat) => {
        //storestat.mtime = parseInt(storestat.mtime / 1000);
        var info = {
            fullpath: fullpath,
            name: filename,
            isFile: stat.isFile(),
            isDirectory: stat.isDirectory(),
            stat: {
                mode: stat.mode,
                uid: stat.uid,
                gid: stat.gid,
                size: stat.size,
                mtime: stat.mtime.getTime()
            }
        };
        return info;
    }).catch(function(error) {
        console.log("Stat error: ");
        console.log(error);
        return null;
    })
}

function safeOpen(path) {
    return Q.ninvoke(fs, 'open', path, 'r').catch((e) => {
        console.log("Error opening: " + path);
        console.log(e);
        return null;
    })
}

function safeReaddir(dirname) {
    return Q.ninvoke(fs, "readdir", dirname).then((dirs) => {
        return dirs;
    }).catch((e) => {
        console.log("Readdir failed for " + dirname + ", returning empty list");
        return [];
    });
}

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

function obj_equals(a, b) {
    for (var key in b) {
        if (a[key] !== b[key]) {
            return false;
        }
    }
    return true;
}

const CHUNK_SIZE = 1048576 * 2 * 2;

function wrap_file(file, cache) {
    function getCachedChunks() {
        return cache.get(file.fullpath).then((olddata) => {
            if (olddata && obj_equals(olddata.stat, file.stat)) {
                return olddata.chunks;
            } else {
                return null;
            }
        });
    }

    function rememberChunks(chunks) {
        return cache.set(file.fullpath, {
            stat: file.stat,
            chunks: chunks
        });
    }

    function forEachChunk(callback) {
        return safeOpen(file.fullpath).then((fd) => {
            if (!fd) {
                return null;
            }

            var accum = [];

            function readNext() {
                var buffer = Buffer.allocUnsafe(CHUNK_SIZE);
                return Q.nfcall(fs.read, fd, buffer, 0, buffer.length, null).then((bytesread) => {
                    bytesread = bytesread[0];
                    buffer = buffer.slice(0, bytesread);
                    if (bytesread !== buffer.length) {
                        throw "BUFFER NOT EQUAL" + [bytesread, buffer.length].join(',');
                    }
                    if (bytesread === 0) {
                        console.log("Read all of file: " + file.fullpath);
                        fs.closeSync(fd);
                        return accum;
                    } else {
                        return callback(buffer).then((res) => {
                            accum.push(res);
                            return readNext();
                        })
                    }
                });
            }
            return readNext();

        });
    }

    file.getCachedChunks = getCachedChunks;
    file.forEachChunk = forEachChunk;
    file.rememberChunks = rememberChunks;
    file.chunkCount = function() {
        return Math.ceil(file.stat.size / CHUNK_SIZE);
    }
    return file;
}

function wrap_dir(dir, cache) {
    function open() {
        return safeReaddir(dir.fullpath).then((dirs) => {
            dirs.sort();
            var ps = dirs.map((d) => {
                return safeStat(path.join(dir.fullpath, d), d);
            });
            // Resolve all those stats
            return Q.all(ps);
        }).then((stats) => {
            // Put the results of stats into file and dir buckets
            var dirs = [];
            var files = [];
            stats.forEach((f) => {
                if (!f) {
                    return;
                } else if (excludes.isGood(f.name) === false) {
                    return null;
                } else if (f.isDirectory) {
                    dirs.push(f);
                } else if (f.isFile) {
                    files.push(f);
                }
            });
            stats = null; // Not necessary, but nulling it out to prevent access

            function forEachDirectory(callback) {
                return genericArrayPromiseIterator(dirs, function(d) {
                    return wrap_dir(d, cache);
                }, callback);
            }

            function forEachFile(callback) {
                return genericArrayPromiseIterator(files, function(f) {
                    return wrap_file(f, cache);
                }, callback);
            }

            return {
                forEachDirectory: forEachDirectory,
                forEachFile: forEachFile,
            }
        })
    }
    return {
        fullpath: dir.fullpath,
        name: dir.name,
        open: open
    };
}

module.exports = {
    wrap_dir: wrap_dir
}