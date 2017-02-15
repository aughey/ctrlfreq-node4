var cf = require('./cf');
var path = require('path');
var Q = require('q')

function walk(dir) {

}

cf.open().then(function(c) {
    var paths = process.argv.slice(0);
    paths.shift();
    paths.shift();
    return Q.all(
        paths.map((p) => {
        	p = path.resolve(p);
        	c.getLastBackup(p).then(walk)
        })
    ).then(() => {
    	c.close();
    });
}).done();