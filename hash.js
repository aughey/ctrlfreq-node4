var crypto = require('crypto');

module.exports = {
	objectHash: function(obj) {
		return module.exports.hash(JSON.stringify(obj));
	},
	hash: function(buffer) {
		var shasum = crypto.createHash('sha1');
		shasum.update(buffer);
		return shasum.digest('hex');
	}
}