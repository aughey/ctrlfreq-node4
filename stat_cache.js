const fs = require('fs');
const Q = require('q');

var stat_cache = null;
try {
    stat_cache = JSON.parse(fs.readFileSync("cache.json").toString());
} catch (e) {
    stat_cache = {};
}

function close() {
    fs.writeFileSync("cache.json", JSON.stringify(stat_cache));
}

function get(key) {
	return Q(stat_cache[key]);
}

function set(key,value) {
	stat_cache[key] = value;
	return Q(true);
}

function eachKey(cb) {
	for(var key in stat_cache) {
		cb(key);
	}
}

module.exports = {
	get: get,
	set: set,
	eachKey: eachKey,
	close: close
}