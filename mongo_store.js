const MongoClient = require("mongodb").MongoClient;
const Q = require('q');
var mongo_url = 'mongodb://localhost:27017/ctrlfreq4';
var hash = require('./hash');
var zlib = require('zlib');
var snappy = require('snappy');

if(process.env.CTRLFREQ4_MONGO) {
    mongo_url = process.env.CTRLFREQ4_MONGO;
}

function compress(data) {
    return Q.ninvoke(snappy,'compress',data).then(function(res) {
        return [res,'s'];
    })
    return Q.ninvoke(zlib,'deflate',data).then(function(res) {
        return [res,'z'];
    })
}

function open(fast) {
    var g_db = null;
    return Q.ninvoke(MongoClient, "connect", mongo_url)
        .then((db) => {
            g_db = db;
            if (!fast) {
                var qs = [];
                qs.push(db.collection('chunks').count());
                qs.push(db.collection('dirs').count());

                return Q.all(qs);
            } else {
                return Q([0, 0, 0]);
            }
        })
        .then((results) => {
            console.log("Chunks: " + results[0]);
            console.log("Directories: " + results[1]);
        })
        .then(function() {
            var db = g_db;
            console.log("Connected to mongo");
            var chunks = db.collection("chunks");
            var dir_collection = db.collection("dirs");

            function isChunkStored(key) {
                return Q(false);

                var cursor = chunks.find({
                    _id: key
                });
                //console.log("finding dir hash: " + key)
                cursor.limit(1)
                return cursor.count().then(function(count) {
                    //console.log("Key: " + key + " returned " + count);
                    if (count > 0) {
                        return true;
                    } else {
                        return false;
                    }
                });
            }

            return {

                hasAllChunks: function(c) {
                    return chunks.find({
                        _id: {
                            $in: c
                        }
                    }).count().then(function(count) {
                        return count === c.length;
                    })
                },
                storeChunk: function(chunk) {
                    var digest = hash.hash(chunk);


                    return isChunkStored(digest).then((isstored) => {
                        if (isstored) {
                            return digest;
                            deferred.resolve(digest);
                        } else {
                            return compress(chunk).then((compressed_buffer) => {
                                var compressed_type = compressed_buffer[1];
                                compressed_buffer = compressed_buffer[0];
                                return chunks.insert({
                                    _id: digest,
                                    stored_on: new Date(),
                                    c: compressed_type,
                                    data: compressed_buffer
                                }, {
                                    continueOnError: true
                                }).then(() => {
                                    return digest;
                                    deferred.resolve(digest);
                                }).catch((e) => {
                                    if (e.code === 11000) {
                                        return digest;
                                    } else {
                                        throw (e);
                                    }
                                });
                            });
                        }
                    });

                },
                storeDirectory: function(fullpath, dirs, files) {
                    var key = ['{fullpath:', JSON.stringify(fullpath), ",dirs:", JSON.stringify(dirs), ",files:", JSON.stringify(files), '}'].join('');
                    var digest = hash.hash(key);

                    return Q(dir_collection.insert({
                        _id: digest,
                        dirs: dirs,
                        files: files,
                        path: fullpath,
                        stored_on: new Date(),
                    }).then(function(res) {
                        console.log("################ " + fullpath);
                        return digest;
                    })).catch((e) => {
                        if (e.code === 11000) {
                            return digest;
                        } else {
                            throw (e);
                        }
                    });
                },
                storeBackup: function(fullpath, root_key) {
                    return db.collection('backups').insert({
                        path: fullpath,
                        stored_on: new Date(),
                        root: root_key
                    }).then((res) => {
                        return {
                            backup_id: res.ops[0]._id,
                            root: root_key
                        };
                    });
                },
                close: function() {
                    var qs = [];
                    qs.push(db.collection('chunks').count());
                    qs.push(db.collection('dirs').count());
                    qs.push(db.stats());
                    return Q.all(qs).then((results) => {
                        console.log("Chunks: " + results[0]);
                        console.log("Directories: " + results[1]);
                        console.log("Stats: " + JSON.stringify(results[2]));
                    }).then(() => {
                        qs.push(db.close());
                    })
                },
                DELETE: function() {
                    return db.dropDatabase('ctrlfreq4');
                }
            };
        });
}

module.exports = {
    open: open
}