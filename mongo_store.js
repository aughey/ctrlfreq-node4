const MongoClient = require("mongodb").MongoClient;
const Q = require('q');
var mongo_url = 'mongodb://localhost:27017/ctrlfreq4';
var hash = require('./hash');
var zlib = require('zlib');
var snappy = require('snappy');

if (process.env.CTRLFREQ4_MONGO) {
    mongo_url = process.env.CTRLFREQ4_MONGO;
}

function compress(data) {
    return Q.ninvoke(snappy, 'compress', data).then(function(res) {
        return [res, 's'];
    })
    return Q.ninvoke(zlib, 'deflate', data).then(function(res) {
        return [res, 'z'];
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
            var chunk_index = db.collection("chunk_index");
            var dir_collection = db.collection("dirs");

            function isChunkStored(key) {
                return hasAllChunks([key]);
            }

            function hasAllChunks(c) {
                var original_count = c.length;
                const max_query = 100;
                var promises = [];
                while (c.length > 0) {
                    var q = c.splice(0, max_query);
                    promises.push(
                        chunk_index.find({
                            _id: {
                                $in: q
                            }},{_id: 1}
                        ).count()
                    );
                }
                return Q.all(promises).then(function(results) {
                    var totalcount = results.reduce((a, b) => a + b, 0);
                    return totalcount === original_count;
                })
            }

            return {
                hasAllChunks: hasAllChunks,
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
                                }).then(() => {
                                    return chunk_index.insert({
                                        _id: digest
                                    });
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