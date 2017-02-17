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

function decompress(data, type) {
    if (type === 's') {
        return Q.ninvoke(snappy, 'uncompress', data);
    } else if (type === 'z') {
        return Q.ninvoke(zlib, 'inflate', data);
    } else {
        throw ("Unknown decompression scheme " + type);
    }
}

function open(fast) {
    var g_db = null;

    function stats() {
        var qs = [];
        qs.push(g_db.collection('chunks').count());
        qs.push(g_db.collection('dirs').count());
        qs.push(g_db.stats());

        return Q.all(qs);
    }

    return Q.ninvoke(MongoClient, "connect", mongo_url)
        .then((db) => {
            g_db = db;
            return db.collection("chunks").createIndex({digest:1},{unique: true});
        }).then(() => {
            return g_db.collection("config").findOne({task: 'created_digest'})
        }).then((res) => {
            if(!res) {
                console.log("Need to create digest first");
                console.log("db.chunks.find({}).forEach((c) => { db.chunks.update({_id: c._id},{digest: c._id}); })")
                throw("STOP");
            }
        })
        .then(function() {
            var db = g_db;
            console.log("Connected to mongo");
            var chunks = db.collection("chunks");
            var chunk_index = db.collection("chunk_index");
            var dir_collection = db.collection("dirs");

            function hasChunk(key) {
                return hasAllChunks([key]);
            }

            function hasAllChunks(c) {
                c = c.slice(0);
                var original_count = c.length;
                const max_query = 100;
                var promises = [];
                while (c.length > 0) {
                    var q = c.splice(0, max_query);
                    promises.push(
                        chunks.find({
                            digest: {
                                $in: q
                            }
                        }, {
                            digest: 1
                        },null).count()
                    );
                }
                return Q.all(promises).then(function(results) {
                    var totalcount = results.reduce((a, b) => a + b, 0);
                    return totalcount === original_count;
                })
            }

            return {
                hasAllChunks: hasAllChunks,
                getChunk: function(digest, do_not_validate) {
                    return chunks.findOne({
                        digest: digest
                    }).then((res) => {
                        return decompress(res.data.buffer, res.c).then((data) => {
                            if(do_not_validate) {
                                return data;
                            } else {
                                var thisdigest = hash.hash(data);
                                if(thisdigest !== digest) {
                                    throw("Digest validate error on getChunk");
                                }
                                return data;
                            }
                        });
                    });
                },
                storeChunk: function(chunk) {
                    var digest = hash.hash(chunk);

                    return hasChunk(digest).then((isstored) => {
                        if (isstored) {
                            return digest;
                        } else {
                            return compress(chunk).then((compressed_buffer) => {
                                var compressed_type = compressed_buffer[1];
                                compressed_buffer = compressed_buffer[0];
                                return chunks.insert({
                                    digest: digest,
                                    stored_on: new Date(),
                                    c: compressed_type,
                                    data: compressed_buffer
                                }).then(() => {
                                    return chunk_index.insert({
                                        _id: digest
                                    });
                                }).then(() => {
                                    return digest;
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
                    // Note new key on 2/17/2017
                    var key = ["fullpath",JSON.stringify(fullpath),"dirs",JSON.stringify(dirs),"files",JSON.stringify(files)].join(',');
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
                getLastBackup: function(fullpath) {
                    return db.collection('backups').find({
                        path: fullpath
                    }, {
                        sort: [
                            ['stored_on', 'descending']
                        ],
                        limit: 1
                    }).toArray().then(function(res) {
                        console.log(res[0]);
                        return res[0].root;
                    })
                },
                getDir: function(key) {
                    return dir_collection.findOne({
                        _id: key
                    }).then((res) => {
                        return {
                            files: res.files,
                            dirs: res.dirs
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
                    return stats().then((res) => {
                        console.log(res);
                    }).then(() => {
                        return db.close();
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