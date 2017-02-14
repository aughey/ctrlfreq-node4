const fs = require('fs');
const Q = require('q');

var levelup = require('levelup')

var db = levelup("./cache");

function close() {
	db.close();
}

function get(key) {
	return Q.ninvoke(db,'get',key).then((val) => {
		return JSON.parse(val);
	}).catch(() => {
		return null;
	})
}

function set(key,value) {
	return Q.ninvoke(db,'put',key,JSON.stringify(value));
}

module.exports = {
	get: get,
	set: set,
	close: close
}