
/*!
 * kue - pool
 * Copyright (c) 2010 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var data = require('../dataconnector.js');

/**
 * Max connections in the pool.
 */

exports.maxConnections = 5;

/**
 * Connection pool.
 */

exports.pool = [];

/**
 * Allocate a redis connection.
 *
 * @return {RedisClient}
 * @api private
 */

exports.alloc = function(){
  var client;
  if (exports.pool.length == exports.maxConnections) {
    client = exports.pool[Math.random() * exports.maxConnections | 0];
  } else {
    exports.pool.push(client = data.createClient());
  }
  return client;
};

/**
 * Return the pubsub-specific  client. 
 *
 * @return {DataClient}
 * @api private
 */

exports.pubsubClient = function(){
  return exports._pubsub || (exports._pubsub = data.createClient());
};

