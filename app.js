const process_dir = require("./process_dir");
const mongo_store = require("./mongo_store");
const path = require('path');
const Q = require('q');

var test_store = {
    need_to_store_file: function() {
        return Q(null);
    },
    storeChunk: function() {
        return Q(null);
    },
    storeFile: function(file, chunk_info) {
        return Q(null);
    },
    storeDirectory: function(dirs, files) {
        return Q(null);
    }
}

if (process.argv[2] === "DELETE") {
    mongo_store.open(true).then(function(store) {
        store.DELETE().then(() => {
            return store.close();
        });
    }).done();

} else {
    mongo_store.open().then(function(store) {
        var fullpath = path.resolve("/jha");
        console.log("Processing")
        return process_dir.process(fullpath, store).then((res) => {
            return store.storeBackup(fullpath,res);
        }).then((backup_key) => {
            console.log("Done with backing up dir.  Backup key: " + JSON.stringify(backup_key));
        }).finally(() => {
            process_dir.close();
            return store.close();
        })
    }).then(() => {
        console.log("DONE");
    }).done();
}