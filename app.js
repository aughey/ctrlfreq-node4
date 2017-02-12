const process_dir = require("./process_dir");
const mongo_store = require("./mongo_store");
const Q = require('q');

var test_store = {
    need_to_store_file: function () {
        return Q(null);
    },
    storeChunk: function () {
        return Q(null);
    },
    storeFile: function (file, chunk_info) {
        return Q(null);
    },
    storeDirectory: function (dirs, files) {
        return Q(null);
    }
}

mongo_store.open().then(function (store) {
    console.log("Processing")
    return process_dir.process("/jha", store).then((res) => {
        console.log("Done with dir");
        console.log(res);
    }).finally(() => {
        store.close();
    })
}).done();