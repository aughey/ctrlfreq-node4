const cf = require("./cf2");
const path = require('path');
const Q = require('q');

if (process.argv[2] === "DELETE") {
    cf.commonOpen().then(function(c) {
        return c.DELETE().then(() => {
            c.close();
        });
    }).done();
} else {
    var paths = process.argv.slice(0);
    paths.shift();
    paths.shift();
    console.log(paths);
    cf.commonOpen().then((c) => {
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