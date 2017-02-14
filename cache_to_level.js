var cache_level = require("./stat_cache_level");
var cache_file = require("./stat_cache");

var keys = [];
cache_file.eachKey((key) => {
	keys.push(key);
});
console.log(keys.length);

function nextkey() {
	if(keys.length === 0) {
		console.log("done");
		return;
	}
	var key = keys.pop();
	return cache_file.get(key).then((value) => {
		console.log(key);
		return cache_level.set(key,value).then(nextkey);
	})
}

nextkey().then(() => {
	cache_level.close();
}).done();