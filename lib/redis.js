
/*!
 * kue - RedisClient factory
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 * Author: bitprobe@gmail.com
 */

/**
 * Module dependencies.
 */

var redis = require('redis');
/**
 * Create a RedisClient
 *
 * @return {RedisClient}
 * @api private
 */
var RedisClient = function() {
    this.executeCommand = function() {
        var args = [];
        for (var arg in arguments)
        args.push(arguments[arg]);
 
        this.client[arguments[0]].apply(this.client,args.slice(1));
    };
 this.client = redis.createClient(6379,'50.57.98.140');
  var getTableData = function(tbCommand) {
      if (tbCommand.tableName == "stats") {
          if (tbCommand.commandType == "update")
            return ["value","q:stats:" + tbCommand.updatedFields[0]];
      }
      tbCommand.filteredFields = [];
      if (tbCommand.tableName == "settings")
        return ["hash","q:settings"];
      if (tbCommand.tableName == "jobtypes")
        return ["set","q:job:types"];
      if (tbCommand.tableName == "jobs") {
          var additional = [];
          var preQuery = null;
         // If we are selected for a status, use the zrange
         if (tbCommand.whereFields.length > 0 ) {
             // Special case: If only contains type, we need to union, then select
         if (tbCommand.whereFields.length == 1 && tbCommand.whereFields[0].name == 'type') {
             var type = tbCommand.whereFields[0].value;
             preQuery = ["unionedset","q:jobs:" + type,"q:jobs:" + type + ":complete","q:jobs:" + type + ":failed","q:jobs:" + type + ":inactive","q:jobs:" + type + ":active","q:jobs:" + type + ":delayed"];   
         }
         
             for (var i = 0; i < tbCommand.whereFields.length; i++) {
                var wField = tbCommand.whereFields[i];
                if (wField.name == 'status')
                    additional.push(wField.value);
                else if (wField.name == 'type')
                    additional.unshift(wField.value);
                    else
                    tbCommand.filteredFields.push(wField);
             }
         }
         
         additional = ":" + additional.join(":");
         if (additional.length == 1)
         additional = "";
        
            if ((tbCommand.selectFields.length == 1 && tbCommand.selectFields[0] == "id")) {
                var type = "sortedset";
                if (tbCommand.filteredFields.length > 0)
                type = "hashlist";
                if (preQuery != null)
                    if (tbCommand.filteredFields.length > 0)
                        return preQuery.concat([[type,"q:jobs" +additional,'q:job',['filter']]]);
                    else
                        return preQuery.concat([[type,"q:jobs" +additional,'q:job' ]]);
                else
                    if (tbCommand.filteredFields.length > 0)
                        return [type,"q:jobs" +additional,'q:job',['filter']];
                    else
                        return [type,"q:jobs" +additional,'q:job'];
            } else if (tbCommand.selectFields.length == 0) {
                // select all
                var type = "sortedset";
                var sAll = ['selectall','q:job'];
                if (tbCommand.filteredFields.length > 0) {
                    sAll = ['filter',['selectall','q:job']];
                    type = "hashlist";
                }
                if (preQuery != null)
                 return preQuery.concat([[type,"q:jobs" +additional,'q:job',sAll]]);
                else
                    return [type,"q:jobs" +additional,'q:job',sAll];
                
                
            } else {
                if (preQuery != null)
                return preQuery.concat([["hashlist","q:jobs" +additional,"q:job" ]]);
                else
                return ["hashlist","q:jobs" + additional,'q:job']   
            }
         }
         
        
  };
  this.execute_set = function(tbData,tbCommand,callback) {
      
      this.executeCommand('smembers',tbData[0],callback);  
  };
  this.execute_unionedset = function(tbData,tbCommand,callback) {
      var args = [];
      args.push('zunionstore');
      args.push(tbData[0]);
      args.push(tbData.length -1);
      var nextCmd = tbData[tbData.length];
      args = args.concat(tbData.slice(1));
      var s = this;
        args.push(callback);
      this.executeCommand.apply(this, args);  
    
  };
  this.execute_filter = function(tbData,tbCommand,callback,originalData, arguments) {
      
     var idList = arguments[1];
    var failed = false;
    var resultList = [];
    var fields = tbCommand.filteredFields;
    

    for (var i = 0; i < idList.length; i++) {
        
        var match = true;
     for (var fi in fields) {
        if (idList[i][fields[fi].name] != fields[fi].value) {
            match = false;
            break;
        }
     }
        
        if (match) {
            
            var id = idList[i];
            if (id.id != undefined)
            id = id.id;
            resultList.push(id);            
        }
    }
    callback(null,resultList);
  };
  this.execute_selectall = function(tbData,tbCommand,callback,originalData, arguments) {
    // Get list from arguments
    
    var idList = arguments[1];
    
    var failed = false;
    var resultList = [];
    var addToList = function(err,data) {
        if (failed == true)
            return;
        if (err != null && err.length > 0) {
            failed = true;
            callback(err,null);
        }
        resultList.push(data);
        if (resultList.length == idList.length)
            callback(err,resultList);
    }
    for (var i = 0; i < idList.length; i++) {
        
     this.executeCommand('hgetall','q:job:' + idList[i], addToList);
    }
  };
  this.execute_value = function(tbData,tbCommand,callback) {
      if (tbCommand.commandType == "update") {
        // If we are adding
        if (tbCommand.addToFields != undefined) {
         this.executeCommand('incrby',tbData[0],tbCommand.addToFields[0].value);
        }
      }
      
  };
  
   this.execute_sortedset = function(tbData,tbCommand,callback,orig,d) {
       
      // if we are getting a count, use zcard
      if (tbCommand.isCount == true)
      this.executeCommand('zcard',tbData[0],callback);
      else {
        this.executeCommand('zrange',tbData[0],0,tbCommand.maxLimit || -1,callback);  
      }
  };
  this.execute_hashlist = function(tbData,tbCommand,callback) {
   var cmd = "";
      var args = [];
      var returnCount = 0;
   
       
       
      // args.push(getTableName(tbCommand.tableName));
          cmd = "sort";
          var hashName = tbData[1];
          args.push(tbData[0]);
          
   if (tbCommand.orderByFields.length > 0)
   { 
       for (var i =0; i < tbCommand.orderByFields.length; i++){
          args.push('by');
          args.push(hashName + ':*->' + tbCommand.orderByFields[i]);
       }
   }
          if (tbCommand.maxLimit != undefined){
          args.push('limit');
          args.push(0);
          args.push(tbCommand.maxLimit);
          }
          var selectFields = [];
          for (var i =0; i < tbCommand.selectFields.length; i++)
          selectFields.push(tbCommand.selectFields[i]);
          for (var f in tbCommand.filteredFields) {
           selectFields.push(tbCommand.filteredFields[f].name);
          }
          
          if (selectFields.length == 0) {
           // Get All
           args.push('get');
           args.push('#');
           returnCount = 1;
          } else {
              args.push('get');
                args.push('#');
              returnCount = selectFields.length+1;
              for (var i = 0; i < selectFields.length; i++) {
               var field = selectFields[i];   
               if (field == 'id')
               continue;
               args.push('get');
               
                args.push(hashName +':*->' + field);
                
              }
          }
      args.push(function(err,jobs) { 
          if (err != null && err.length > 0)
            return callback(err,undefined);
            
          var jobsToReturn = [];
           while (jobs.length) {
            var job = jobs.slice(0, returnCount);
            var newJob = {};
            if (job[0].id == undefined)
            newJob.id = job[0];
            else
            newJob = job[0];
            var selectedFields = [].concat(tbCommand.selectFields);
            for (var f in tbCommand.filteredFields)
            selectedFields.push(tbCommand.filteredFields[f].name);
            for (var i = 0; i <  selectedFields.length; i++) {
             newJob[selectedFields[i]] = job[i+1];   
            }
            jobsToReturn.push(newJob);
            jobs = jobs.slice(returnCount);
           }
           callback(err,jobsToReturn);
          });
        // if id
        if (tbCommand.filteredFields.length > 0 && tbCommand.filteredFields[0].name == 'id') {
            this.executeCommand.apply(this,["hgetall","q:job:" + tbCommand.filteredFields[0].value,callback]);
        } else {
        this.executeCommand.apply(this,[cmd].concat(args));
        }
  };
  this.execute_hash = function(tbData,tbCommand,callback) {
      this.client.executeCommand('hgetall',tbData[0],callback);
  };
  this.executeFromTableData = function(tbData,tbCommand,callback, originalData, callbackArgs) {
    
    var lastArg = tbData[tbData.length - 1];
      // if final arg is array, it's a multi-step command
      if (typeof lastArg == 'object' && lastArg.push != undefined) {
       var executeFn = this["execute_" + tbData[0]];
       var _this = this;
           
       executeFn.call(this,tbData.slice(1,tbData.length-1),tbCommand,function() {
           
           _this.executeFromTableData(lastArg,tbCommand,callback,tbData,arguments);
        },originalData, callbackArgs);   
      } else {
          
       var executeFn = this["execute_" + tbData[0]];
       executeFn.call(this,tbData.slice(1),tbCommand,callback,originalData, callbackArgs);
      }  
  };
 // this.redis = redis.createClient();
  this.execute = function(tbCommand,callback) {
      
      var tbData = getTableData(tbCommand);
      
      this.executeFromTableData(tbData,tbCommand,callback);
      
   
  };
};
exports.Client = RedisClient;

