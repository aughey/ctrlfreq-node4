var cf = require('./cf');

cf.open().then(function(c) {
	console.log("Walking");
	return c.close();
});