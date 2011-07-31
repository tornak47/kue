
/*!
 * kue
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter
  , Worker = require('./queue/worker')
  , events = require('./queue/events')
  , Job = require('./queue/job')
  , dataConnector = require('./dataconnector.js');

/**
 * Expose `Queue`.
 */

exports = module.exports = Queue;

/**
 * Library version.
 */

exports.version = '0.2.0';

/**
 * Expose `Job`.
 */

exports.Job = Job;

/**
 * Server instance (that is lazily required)
 */

var app;

/**
 * Expose the server.
 */

Object.defineProperty(exports, 'app', {
  get: function() {
    return app || (app = require('./http'));
  }
});

/**
 * Expose the RedisClient factory.
 */

exports.dataConnector = dataConnector;

/**
 * Create a new `Queue`.
 *
 * @return {Queue}
 * @api public
 */

exports.createQueue = function(){
  return new Queue;
};

/**
 * Initialize a new job `Queue`.
 *
 * @api public
 */

function Queue() {
  this.client = dataConnector.createClient();
}

/**
 * Inherit from `EventEmitter.prototype`.
 */

Queue.prototype.__proto__ = EventEmitter.prototype;

/**
 * Create a `Job` with the given `type` and `data`.
 *
 * @param {String} type
 * @param {Object} data
 * @return {Job}
 * @api public
 */

Queue.prototype.create =
Queue.prototype.createJob = function(type, data){
  return new Job(type, data);
};

/**
 * Promote delayed jobs, checking every `ms`,
 * defaulting to 5 seconds.
 *
 * @params {Number} ms
 * @api public
 */

Queue.prototype.promote = function(ms){
  var client = this.client
    , ms = ms || 5000
    , limit = 20;

  setInterval(function(){
     client.from("jobs").where('status','delayed').orderBy("delay").select("id","created_at","delay").limit(1)
     .execute(function(err,jobs) { 
         
    
      if (err || !jobs.length) return;

      // iterate jobs with [id, delay, created_at]
      while (jobs.length) {
        var job = jobs.pop()
          , id = parseInt(job.id, 10)
          , delay = parseInt(job.delay, 10)
          , creation = parseInt(job.created_at, 10)
          , promote = ! Math.max(creation + delay - Date.now(), 0);

        // if it's due for activity
        // "promote" the job by marking
        // it as inactive.
        if (promote) {
          Job.get(id, function(err, job){
            if (err) return;
            events.emit(id, 'promotion');
            job.inactive();
          });
        }
      }
    });
  }, ms);
};

/**
 * Get setting `name` and invoke `fn(err, res)`.
 *
 * @param {String} name
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.setting = function(name, fn){
    
  this.client.from('settings').select(name).execute(fn);
  return this;
};

/**
 * Process jobs with the given `type`, invoking `fn(job)`.
 *
 * @param {String} type
 * @param {Number|Function} n
 * @param {Function} fn
 * @api public
 */

Queue.prototype.process = function(type, n, fn){
  var self = this;

  if ('function' == typeof n) fn = n, n = 1;

  while (n--) {
    (function(worker){
      worker.on('error', function(err){
        self.emit('error', err);
      });

      worker.on('job complete', function(job){
          //self.client.incrby('q:stats:work-time', job.duration);
        self.client.update('stats').add('work-time',job.duration).execute();
      });
    })(new Worker(this, type).start(fn));
  }
};

/**
 * Get the job types present and callback `fn(err, types)`.
 *
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.types = function(fn){
  this.client.from('jobtypes').execute(fn);
  return this;
};

/**
 * Return job ids for the given `status`, and
 * callback `fn(err, ids)`.
 *
 * @param {String} status
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.status = function(status, fn){
    //this.client.zrange('q:jobs:' + status, 0, -1, fn);
  this.client.from('jobs').where('status',status).execute(fn);
  return this;
};

/**
 * Get count of `status` and callback `fn(err, n)`.
 *
 * @param {String} status
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.count = function(status, fn){
  this.client.from('jobs').where('status',status).count(fn);
  return this;
};

/**
 * Completed jobs.
 */

Queue.prototype.complete = function(fn){
  return this.status('complete', fn);
};

/**
 * Failed jobs.
 */

Queue.prototype.failed = function(fn){
  return this.status('failed', fn);
};

/**
 * Inactive jobs (queued).
 */

Queue.prototype.inactive = function(fn){
  return this.status('inactive', fn);
};

/**
 * Active jobs (mid-process).
 */

Queue.prototype.active = function(fn){
  return this.status('active', fn);
};

/**
 * Completed jobs count.
 */

Queue.prototype.completeCount = function(fn){
  return this.count('complete', fn);
};

/**
 * Failed jobs count.
 */

Queue.prototype.failedCount = function(fn){
  return this.count('failed', fn);
};

/**
 * Inactive jobs (queued) count.
 */

Queue.prototype.inactiveCount = function(fn){
  return this.count('inactive', fn);
};

/**
 * Active jobs (mid-process).
 */

Queue.prototype.activeCount = function(fn){
  return this.count('active', fn);
};

/**
 * Delayed jobs.
 */

Queue.prototype.delayedCount = function(fn){
  return this.count('delayed', fn);
};
