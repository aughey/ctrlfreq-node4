const cf = require("./cf");
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
    cf.open().then(function(c) {
        return c.DELETE().then(() => {
            c.close();
        });
    }).done();
} else {
    var paths = process.argv.slice(0);
    paths.shift();
    paths.shift();
    console.log(paths);
    cf.open().then((c) => {
        return Q.all(
            paths.map((fullpath) => {
                var fullpath = path.resolve(fullpath);
                console.log("Processing " + fullpath)
                return c.process(fullpath).then((backup_key) => {
                    console.log("Done with backing up dir.  Backup key: " + JSON.stringify(backup_key));
                });
            })
        ).then(() => {
            return c.close();
        });
    }).then(() => {
        console.log("DONE");
    }).done();
}