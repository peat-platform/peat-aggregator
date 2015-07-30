/**
 * Created by dconway on 16/02/15.
 * Licensed under the MIT license.
 */


'use strict';

var dbc          = require('dbc');
var peatLogger  = require('peat-logger');
var peatUtils   = require('cloudlet-utils');
var url          = require('url');
var zmq          = require('m2nodehandler');
var couchbase    = require('couchbase');
var N1qlQuery    = require('couchbase').N1qlQuery;
var squel        = require("squel");


var logger   = null;
var cluster  = new couchbase.Cluster( 'couchbase://localhost' );
var bucket   = cluster.openBucket('objects');
bucket.enableN1ql('localhost:8093');


var test = function(msg, callback) {

   var n1ql = squel.select().from("objects")

   //n1ql.field("`@data` as data")


   if ( undefined !== msg.json["types"]){
      var where = squel.expr()
      for (var i in msg.json.types){
         where.or('`@type`="' + msg.json["types"][i] + '"')
      }
      n1ql.where(where);
   }


   if ( undefined !== msg.json["fields"]){
      for (var i in msg.json.fields){
         n1ql.field(msg.json["fields"][i])
      }
   }

   if ( undefined !== msg.json["where"]){
      for (var i in msg.json.where){
         n1ql.where(msg.json["where"][i])
      }
   }

   n1ql.where('`_permissions`.`' + msg.token.cloudlet + '`.`read` = true')

   console.log(n1ql.toString());

   var query = N1qlQuery.fromString(n1ql.toString());

   //console.log("query", query)

   bucket.query(query, function(err, result) {

      if (err){
         callback(null,{'Error: ' : err})
      }

      console.log("err", err);
      console.log("result", result.length);

      if (result.length !== 1){
         callback({"error" : "Only querys that result in single results allowed. Found: " + result.length })
      }
      else{
         var resp = [];

         console.log("result", result[0]);

         if ( undefined !== msg.json["fields"]){
            for (var i = 0; i < msg.json.fields.length; i++){
               resp[i]           = {};
               resp[i]["field"]  = msg.json["fields"][i];
               resp[i]["result"] = result[0][ "$" + (i + 1)];
               console.log("$ + (i + 1)",  "$" + (i + 1));
               //where.or('`@type`="' + msg.json["fields"][i] + '"')
            }
         }

         callback(resp)
      }
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


module.exports.processMongrel2Message = processMongrel2Message;