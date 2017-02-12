var fs = require('fs');
var Q = require('q');
var path = require('path');
var excludes = require('./file_excludes');
var hash = require('./hash');

function chunkFile(file, store) {
    // Chunkfile could safely error out, so we create our own deferral
    var deferred = Q.defer();

    var g_fd = null;
    Q.ninvoke(fs, 'open', file.fullpath, 'r').then((fd) => {
        g_fd = fd;
        console.log("Opened: " + file.fullpath + " ");

        var chunks = [];

        function nextChunk() {
            var size = 1048576;
            var buffer = new Buffer(size);
            return Q.nfcall(fs.read, fd, buffer, 0, size, null).then((bytesread) => {
                bytesread = bytesread[0];
                if (bytesread === 0) {
                    console.log("Read all of file: " + file.fullpath);
                    store.storeFile(file,chunks).then((f) => {
                        deferred.resolve(f);
                    }).done();
                } else {
                    return store.storeChunk(buffer).then(function(chunk_data) {
                        chunks.push(chunk_data);
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
        console.log(error);
        deferred.resolve(null);

    }).done();

    return deferred.promise;
}

function processFile(file, store) {
    // console.log("Processing file: " + file.fullpath);
    return store.need_to_store_file(file).then((store_cache) => {
        if (store_cache) {
            return store_cache;
        } else {
            return chunkFile(file, store);
        }
    })
}

function safeStat(fullpath) {
    // We use our own promise here because we might catch
    // and handle a stat error
    var deferred = Q.defer();

    Q.nfcall(fs.stat, fullpath).then((s) => {
        deferred.resolve(s);
    }).catch(function (error) {
        console.log("Stat error: " + error);
        console.log(typeof error);
        console.log(error);
        deferred.resolve(null);
    }).done();

    return deferred.promise;
}

function process(dirname, store) {
    dirname = path.resolve(dirname);
    var dirobj = {
        path: dirname
    };
    console.log("Processing dir: " + dirname);

    return Q.ninvoke(fs, "readdir", dirname).then((dirs) => {
        var stats = dirs.map((file) => {
            var fullpath = path.join(dirname, file);

            if (excludes.isGood(file) === false) {
                return null;
            }

            return safeStat(fullpath).then((stat) => {
                if (!stat) {
                    return null;
                }
                var storestat = {
                    mode: stat.mode,
                    uid: stat.uid,
                    gid: stat.gid,
                    size: stat.size,
                    mtime: stat.mtime
                }
                storestat.mtime = storestat.mtime / 1000;
                var info = {
                    fullpath: fullpath,
                    file: file,
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

        var stored_files = [];
        function nextFile(laststore) {
            if(laststore) {
                stored_files.push(laststore);
            }
            var file = files.pop();
            if (!file) {
                return store.storeDirectory(dirname,stored_dirs,stored_files);
            } else {
                return processFile(file, store).then(nextFile);
            }
        }

        var stored_dirs = [];
        function nextDir() {
            var dir = dirs.pop();
            if (!dir) {
                // Done with dirs, do files
                return nextFile();
            } else {
                return process(dir.fullpath, store).then((s) => {
                    if(s) {
                        stored_dirs.push(s);
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