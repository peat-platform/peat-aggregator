/**
 * Created by dconway on 16/02/15.
 * Licensed under the MIT license.
 */


'use strict';

var dbc          = require('dbc');
var openiLogger  = require('openi-logger');
var openiUtils   = require('openi-cloudlet-utils');
var url          = require('url');
var zmq          = require('m2nodehandler');
var couchbase    = require('couchbase');
var N1qlQuery    = require('couchbase').N1qlQuery;


var logger   = null;
var cluster  = null;
var bucket   = null;
var dbs      = {};


var init = function(logger_params){
   if(logger == undefined)
      logger = openiLogger(logger_params);

   if(cluster == undefined)
      cluster = new couchbase.Cluster( 'couchbase://localhost' );

   dbs['objects']     = cluster.openBucket('objects');
   dbs['types']       = cluster.openBucket('types');
   dbs['attachments'] = cluster.openBucket('attachments');
   dbs['permissions'] = cluster.openBucket('permissions');

   bucket = dbs['objects'];
   //bucket.enableN1ql(['http://192.168.33.1:8093','http://127.0.0.1:8093','http://192.168.33.10:8093']);
   bucket.enableN1ql('127.0.0.1:8093');
   //console.log(bucket)
};

var aggregate = function(responce, callback) {
   var resp = {};
   resp.count = responce.length;
   resp.entities = responce;
   callback(resp)
};

var test = function(msg, callback) {
   var qstring = "SELECT * FROM objects where _permissions."+msg.token.cloudlet+".read=true";
   console.log(qstring);
   var query = N1qlQuery.fromString(qstring);
   bucket.query(query,
      function(err, result) {
         if (err){
            callback(null,{'Error: ' : err})
         }
         aggregate(result,callback)
      });

};


var processMongrel2Message = function (msg, senderToClient) {

   //this.logger.log('debug', 'process Mongrel 2 Message function');

   test(msg,function(result,err){
      if (!err){
         senderToClient.send(msg.uuid, msg.connId, zmq.status.OK_200, zmq.standard_headers.json, result);
      }
      else {
         senderToClient.send(msg.uuid, msg.connId, zmq.status.INTERNAL_SERVER_ERROR, zmq.standard_headers.json, err);
      }
   })




};


module.exports.init                   = init;
module.exports.processMongrel2Message = processMongrel2Message;