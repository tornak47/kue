
/*!
 * kue - DataConnector factory
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 * Author: bitprobe@gmail.com
 */


/**
 * Create a RedisClient
 *
 * @return {RedisClient}
 * @api private
 */
 var TableCommand = function(commandType, tbName,client) {
   this.tableName = tbName;
   this.commandType = commandType;
   this.client = client;
   this.orderByFields = new Array();
   this.selectFields = new Array();
   this.whereFields = new Array();
   this.updatedFields = new Array();
   this.orderBy = function(orderByField) {
      this.orderByFields.push(orderByField);
      return this;
   };
   this.limit = function(limit) {
    this.maxLimit = limit;   
    return this;
   };
   
   this.select = function() {
     this.selectFields = arguments;  
     return this;
   };
   this.where = function(name,value) {
     this.whereFields.push({name:name,value:value});
     return this;
   };
   this.count = function(callback) {
     this.isCount = true;
     this.execute(callback);
   };
   if (this.commandType == "update") {
   this.add = function(key,value) { 
       if (this.addToFields == undefined)
       this.addToFields = new Array();
       this.updatedFields.push(key);
       this.addToFields.push({name:key,value:value});
       return this;
   };
   }
   this.execute = function(callback) {
     // Execute this command
     
     return client.execute(this,callback);  
   };
 };
 var ClientBase =  { 
    from: function(tableName) {
        return new TableCommand("from",tableName,this);        
    },
    update: function(tableName) {
        return new TableCommand("update",tableName,this);        
    },
    execute: function(command) {
     throw 'Client must override execute';   
    }
 }
 var redisClient = require('./redis.js').Client;
 redisClient.prototype = ClientBase;
 exports.createClient = function() { 
     
  return new redisClient();   
 }