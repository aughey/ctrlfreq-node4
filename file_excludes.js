var path = require('path');

var extensions = "wab~,vmc,vhd,vo1,vo2,vsv,vud,vmdk,vmsn,vmsd,hdd,vdi,vmwarevm,nvram,vmx,vmem,iso,dmg,sparseimage,sys,cab,exe,msi,dll,dl_,wim,ost,o,qtch,log,tmp";

extensions = extensions.split(',');
extensions = extensions.map(function (p) {
    return "." + p;
});
var badfiles = [
    ".DS_Store",
    "Thumbs.db"
];
badfiles = badfiles.map(function (f) {
    return f.toLowerCase();
});

function isGood(file) {
    exclude_count = 0;

    var file = file.toLowerCase();
    var ext = path.extname(file);

    if(extensions.includes(ext)) {
        return false;
    }
    if(badfiles.includes(file)) {
        return false;
    }
    if(file[0] === '~') {
        return false;
    }
    return true;
}

module.exports = {
    isGood: isGood
}