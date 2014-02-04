var https  = require('https'),
    path   = require('path'),
    fs     = require('fs'),
    util   = require('util'),
    events = require('events'),

    aws = require('aws-sdk'),

    q         = require('q'),
    _         = require('lodash'),
    glob      = require('glob'),
    yargs     = require('yargs'),

    s3       = require('./s3'),
    utils    = require('./utils');


aws.config.loadFromPath('./aws.json');

function getBucketList(bucket) {
   var client = new aws.S3();

   function _getBucketListSegment(marker) {
      var list_opts = {'Bucket': bucket};

      if (marker !== undefined) {
         list_opts['Marker'] = marker;
      }

      return q.ninvoke(client, 'listObjects', list_opts)
         .then(function(data) {
            var items = data.Contents;

            if (data.IsTruncated) {
               var last_item = _.last(items);
               return _getBucketListSegment(last_item.Key)
                  .then(function(nextItems) {
                     return items.concat(nextItems);
                  });
            }

            return items;
         }, function(err) {
            console.error(err);
            return err;
         });
   }

   return _getBucketListSegment();
}


function getBucketItemsByMd5(bucket) {
   return getBucketList(bucket)
      .then(function(objects) {
         var md5_to_object = _.indexBy(objects, function(object) {
            return object.ETag.slice(1, -1);
         });
         return md5_to_object;
      });
}


function uploadFileToS3(bucket, maxUploads, filepath, md5) {
   var key = md5 + path.extname(filepath);

   return q.ninvoke(fs, 'readFile', filepath)
      .then(function(buffer) {
         var agent = new https.Agent();
         agent.maxSockets = maxUploads + 1; // Add one just encase the JobBus is stupid
         var client = new aws.S3({
            params:      { Bucket: bucket },
            httpOptions: { agent:  agent }
         });

         return q.ninvoke(client, 'putObject', {
            Body:   buffer,
            Key:    md5 + path.extname(filepath)
         });
      });
}


function JobBus() {
   this.active = 0;
}

JobBus.prototype.__proto__ = events.EventEmitter.prototype;

JobBus.prototype.doWork = function(func) {
   var emitter = this;

   emitter.emit('workstart');

   this.active = this.active + 1;

   function emitDone(res) {
      emitter.active = emitter.active - 1;
      emitter.emit('workdone', res);
      return res;
   }

   return func()
      .then(emitDone, emitDone);
};


function uploadFilesToS3(args, files) {
   var bus         = new JobBus(),
       max_uploads = args['max-uploads'],
       bucket      = args.bucket,
       ordered_files = _.pairs(files);

   var d = q.defer(),
       work_finished = 0;

   function add_work(opts) {
      var filepath = opts[1],
          md5      = opts[0];
      bus
         .doWork(function() {
            return uploadFileToS3(args.bucket, max_uploads, filepath, md5);
         })
         .catch(function(err) { // Catch any errors so that they do not propogate
            console.error(err);
         })
         .done();
   }

   bus.on('workdone', function(res) {
      work_finished  = work_finished + 1;
      var percent = work_finished / ordered_files.length;
      util.print(util.format('\rUploading %d%%', (percent * 100).toFixed(1)));

      var started_work = work_finished + bus.active;
      if (started_work< ordered_files.length) {
         add_work(ordered_files[started_work]);
      }
      else if (work_finished === files.length) {
         util.print('\n');
         d.resolve(null);
      }

      return res;
   });

   util.print(util.format('Uploading 0%%'));

   // Kick start the work
   _(ordered_files).first(max_uploads).each(add_work);

   return d.promise;
}


function getMd5ForFiles(args, files) {
   var max_read = args['max-file-read'],
       bus  = new JobBus(),
       md5s = {},
       d    = q.defer();

   function add_work(filepath) {
      bus.doWork(function() {
         return utils.computeFileHash(path.resolve(filepath), 'md5')
            .then(function(md5) {
               md5s[md5] = filepath;
            });
      }).done();
   }

   var work_finished = 0;
   bus.on('workdone', function(res) {
      work_finished  = work_finished + 1;
      var percent = work_finished / files.length;
      util.print(util.format('\rComputing MD5s %d%%', (percent * 100).toFixed(1)));

      var started_work = work_finished + bus.active;
      if (started_work< files.length) {
         add_work(files[started_work]);
      }
      else if (work_finished === files.length) {
         util.print('\n');
         d.resolve(md5s);
      }
   });

   // Kick start the work
   _(files).first(max_read).each(add_work);

   return d.promise;
}


function getFilesByMd5(args) {
   return q.nfcall(glob, args['search-glob'])
      .then(function(files) {
         return getMd5ForFiles(args, files);
      });
}


function getArgs() {
   return yargs
      .demand('search-glob')
      .describe('search-glob', 'Glob to match files to upload')
      .demand('bucket')
      .describe('bucket', 'S3 Bucket name to upload files to')
      .describe('dry-run', 'Show what would be done')
      .default('dry-run', false)
      .describe('dry-run', 'Show what would be done')
      .describe('v', 'Enable verbose mode')
      .default('max-uploads', 10)
      .describe('max-uploads', 'Maximum number of active uploads allowed at a time.')
      .default('max-file-read', 2)
      .describe('max-file-read', 'Maximum number of files to read at a time.');
}


function main() {
   var args = getArgs().argv;

   if (args.help) {
      args.showHelp();
      return;
   }

   q.all([getBucketItemsByMd5(args.bucket), getFilesByMd5(args)])
      .then(function(results) {
         var s3_md5s  = results[0],
             dir_md5s = results[1];

         console.log('Found');
         console.log(' - Files S3:   ', _.keys(s3_md5s).length);
         console.log(' - Files Local:', _.keys(dir_md5s).length);

         // Find the files not yet on S3
         var md5s_to_upload = _.difference(_.keys(dir_md5s), _.keys(s3_md5s));

         if (args['dry-run']) {
            console.log('Would upload %d files', _.keys(md5s_to_upload).length);
            return [];
         }
         else {
            console.log('Will Upload:', _.keys(md5s_to_upload).length);
            var files = _(md5s_to_upload)
                   .map(function(md5) {return [md5, dir_md5s[md5]];})
                   .zipObject().value();
            return uploadFilesToS3(args, files);
         }
      })
      .done();
}

// Launch the program
if (require.main === module) {
    main();
}
