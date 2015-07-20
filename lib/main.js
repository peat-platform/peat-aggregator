/**
 * Created by dconway on 16/02/15.
 * Licensed under the MIT license.
 */


'use strict';

var helper            = require('./helper.js');
var path              = require('path');
var jwt               = require('jsonwebtoken');
var zmq               = require('m2nodehandler');



var openi_aggregator = function(config){

   var senderToClient = zmq.sender(config.mongrel_handler.sink);

   zmq.receiver(config.mongrel_handler.source, config.mongrel_handler.sink, function(msg) {

      //helper.processMongrel2Message(msg, senderToClient, config.mongrel_handler.sink);

      if ( undefined === msg.headers.authorization ){
         senderToClient.send(msg.uuid, msg.connId, zmq.status.BAD_REQUEST_400, zmq.standard_headers.json, {"error":"Missing Auth token: " });
         return
      }

      var tokenB64 = msg.headers.authorization.replace("Bearer ", "");

      jwt.verify(tokenB64, config.trusted_security_framework_public_key, function(err, token) {

         if (undefined !== err && null !== err){
            senderToClient.send(msg.uuid, msg.connId, zmq.status.BAD_REQUEST_400, zmq.standard_headers.json, {"error":"Invalid token: " + err });
         }
         else if (token.scope !== "developer"){
            senderToClient.send(msg.uuid, msg.connId, zmq.status.BAD_REQUEST_400, zmq.standard_headers.json, {"error":"Invalid token type, must be session token with developer scope " });
         }
         else {
            if (token){
               msg.token = token;
            }
            helper.processMongrel2Message(msg, senderToClient, config.mongrel_handler.sink);
         }
      });
   });
}


module.exports = openi_aggregator;