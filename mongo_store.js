const MongoClient = require("mongodb").MongoClient;
const Q = require('q');
const mongo_url = 'mongodb://localhost:27017/ctrlfreq4';
var hash = require('./hash');


function open() {
    var g_db = null;
    return Q.ninvoke(MongoClient, "connect", mongo_url).then(function(db) {
        g_db = db;
        console.log("Connected to mongo");
        var chunks = db.collection("chunks");
        var file_collection = db.collection("files");
        var dir_collection = db.collection("dirs");

        function isChunkStored(key) {
			var cursor = chunks.find({
				hash: key
			});
			//console.log("finding dir hash: " + key)
			cursor.limit(1)
			return Q.ninvoke(cursor, 'count').then(function(count) {
				//console.log("Key: " + key + " returned " + count);
				if (count > 0) {
					return true;
				} else {
					return false;
				}
			});
		}

        return {
            need_to_store_file: function(file) {
                return Q.ninvoke(file_collection,'findOne',{
                    unique_id: file.unique_id
                }).then(function(res) {
                    if(res) {
                        return res._id;
                    } else {
                        return null;
                    }
                })
            },
            storeChunk: function(chunk) {
                var digest = hash.hash(chunk);

                return isChunkStored(digest).then((isstored) => {
                    if(isstored) {
                        console.log("Chunk is already stored");
                        return digest;
                    } else {
                        console.log("Insertting chunk");
                        return Q.ninvoke(chunks,'insert',{
                            hash: digest,
                            data: chunk
                        }).then(() => {
                            return digest;
                        });
                    }
                });
            },
            storeFile: function(info,chunks) {
                return Q.ninvoke(file_collection,'insert',{
                    path: info.fullpath,
                    unique_id: info.unique_id,
                    info: info,
                    chunks: chunks,
                    stored_on: new Date(),
                }).then(function(res) {
                    console.log(res);
                    return res.insertedIds[0];
                });
            },
            storeDirectory: function(fullpath,dirs,files) {
                return Q.ninvoke(dir_collection,'insert',{
                    dirs: dirs,
                    files: files,
                    path: fullpath,
                    stored_on: new Date(),
                }).then(function(res) {
                    return res.insertedIds[0];
                });
            },
            close: function() {
                db.close();
            }
        };     
    });
}

module.exports = {
    open: open
}