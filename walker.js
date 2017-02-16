var cf = require('./cf');
var path = require('path');
var Q = require('q')
var fs = require('fs');

function checkFile(f, indent) {
    console.log(indent + f.fullpath);
    var g_fd;
    var bytes_remaining;
    return Q.ninvoke(fs, 'open', f.fullpath, 'r').then(function(fd) {
        g_fd = fd;
        var stat = fs.fstatSync(fd);
        bytes_remaining = stat.size;
        return f.eachChunk((chunk) => {
            console.log(chunk.length);
            var buffer = Buffer.allocUnsafe(chunk.length);
            return Q.ninvoke(fs, 'read', fd, buffer, 0, buffer.length, null).then(function(data) {
                data = data[1];
                if (0 !== Buffer.compare(data, chunk)) {
                    throw ("Not equal: " + f.fullpath);
                }
                bytes_remaining -= buffer.length;
            });
        })
    }).finally(() => {
        if (bytes_remaining !== 0) {
        	console.log(f);
            throw ({
                e: "We didn't read all of the file: " + bytes_remaining + " " + f.fullpath,
                data: f
            });
        }
        fs.close(g_fd);
    })
    return Q(true);
}

function walk(dir, indent) {
    if (!indent) {
        indent = "";
    }
    console.log(indent + dir.name);
    indent = indent + "  ";

    return dir.eachFile((f) => {
        return checkFile(f, indent);
    }).then(() => {
        return dir.eachDir((d) => {
            return walk(d, indent);
        });
    })
}

cf.open().then(function(c) {
    var paths = process.argv.slice(0);
    paths.shift();
    paths.shift();
    return Q.all(
        paths.map((p) => {
            p = path.resolve(p);
            return c.getLastBackup(p).then(walk)
        })
    ).then(() => {
        console.log("CLOSING");
        c.close();
    });
}).done();