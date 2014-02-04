var crypto = require('crypto'),
    fs     = require('fs'),
    util   = require('util'),

    q = require('q');


var computeHash = exports.computeHash = function(str, algo) {
   algo = algo || 'md5';

   var hasher = crypto.createHash(algo);
   hasher.update(str);
   return hasher.digest('hex');
};


exports.computeFileHash = function(filepath, algo) {
   algo = algo || 'md5';

   var d = q.defer();

   var hasher = crypto.createHash(algo);

   var s = fs.ReadStream(filepath);
   s.on('data', function(d) {
      hasher.update(d);
   });

   s.on('error', function(err) {
      d.reject(err);
   });

   s.on('end', function() {
      d.resolve(hasher.digest('hex'));
   });

   return d.promise;
};
