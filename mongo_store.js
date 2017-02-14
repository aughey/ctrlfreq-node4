const MongoClient = require("mongodb").MongoClient;
const Q = require('q');
const mongo_url = 'mongodb://localhost:27017/ctrlfreq4';
var hash = require('./hash');


function open(fast) {
    var g_db = null;
    return Q.ninvoke(MongoClient, "connect", mongo_url)
        .then((db) => {
            g_db = db;
            if (!fast) {
                var qs = [];
                qs.push(db.collection('chunks').count());
                qs.push(db.collection('files').count());
                qs.push(db.collection('dirs').count());
                qs.push(db.collection('files').createIndex({
                    unique_id: 1
                }, {
                    unique: true
                }));

                return Q.all(qs);
            } else {
                return Q([0, 0, 0]);
            }
        })
        .then((results) => {
            console.log("Chunks: " + results[0]);
            console.log("Files: " + results[1]);
            console.log("Directories: " + results[2]);
        })
        .then(function() {
            var db = g_db;
            console.log("Connected to mongo");
            var chunks = db.collection("chunks");
            var file_collection = db.collection("files");
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
                    return chunks.find({_id: { $all:  c }}).count().then(function(count) {
                        console.log(count + " " + c.length);
                        return count === c.length;
                    })
                },
                storeChunk: function(chunk) {
                    var digest = hash.hash(chunk);

                    // Storing data has an unavoidable race condition
                    // that can cause an exception that we will catch
                    // here

                    var deferred = Q.defer();

                    return isChunkStored(digest).then((isstored) => {
                        if (isstored) {
                            return digest;
                            deferred.resolve(digest);
                        } else {
                            return chunks.insert({
                                _id: digest,
                                data: chunk
                            }, {
                                continueOnError: true
                            }).then(() => {
                                return digest;
                                deferred.resolve(digest);
                            }).catch((e) => {
                                //  console.log("!!!! inside catch");
                                //                             deferred.resolve(digest);
                                return digest;
                            })
                        }
                    });

                },
                storeFile: function(info, chunks) {
                    return Q(file_collection.insert({
                        unique_id: info.unique_id,
                        info: info,
                        chunks: chunks,
                        stored_on: new Date(),
                    }).then(function(res) {
                        return res.insertedIds[0];
                    }));
                },
                storeDirectory: function(fullpath, dirs, files) {
                    return Q(dir_collection.insert({
                        dirs: dirs,
                        files: files,
                        path: fullpath,
                        stored_on: new Date(),
                    }).then(function(res) {
                        return res.insertedIds[0];
                    }));
                },
                close: function() {
                    var qs = [];
                    qs.push(db.collection('chunks').count());
                    qs.push(db.collection('files').count());
                    qs.push(db.collection('dirs').count());
                    qs.push(db.close());
                    return Q.all(qs).then((results) => {
                        console.log("Chunks: " + results[0]);
                        console.log("Files: " + results[1]);
                        console.log("Directories: " + results[2]);
                    });
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