var calculateSlot = require('cluster-key-slot');

var sketch = {slots:{}};
var lastStore = undefined;
var redis = undefined;
var addedClientToRedis = false;

const { DDSketch } = require('sketches-js');

var optimizationMetric = '99'
// var optimizationMetric = 'avg'

//this is for a single cluster
//sketch one window at a time?

module.exports.printLog = function(cmd, elapsed) {
  Object.keys(sketch).forEach(function(node) {
    var cnt = sketch[node].cnt
    var latency = getRequestLatency(node);
    console.log("Accessed", node, cmd, cnt, "times. Took", latency, "ms...");
  });
  console.log("Accessed ", Object.keys(sketch.slots).length, " slots:", Object.keys(sketch.slots));
};

var getStorableSketch = function() {
  var storableSketch = {slots:{}}
  Object.keys(sketch).forEach(function(node) {
    if(node == 'time' || node == 'slots') {
      storableSketch[node] = sketch[node]
    } else {
      storableSketch[node] = {
        cnt: sketch[node].cnt,
        sum: getRequestLatency(node) * sketch[node].cnt
      }
    }
  });
  return JSON.stringify(storableSketch);
}

var storing = false;
var clientID = require('uuid/v1')();
var storeSketch = function() {
  //check if you're already in the middle of doing this...
  if(storing) return;
  storing = true;

  sketch['time'] = Date.now()

  //store sketch
  redis.setex(clientID, 600, getStorableSketch(), function(err, resp) {
    if (err) console.log(err);
    else {
      console.log("set sketch into redis.");
      lastStore = process.hrtime();

      //ensure client is stored in the client-key
      if (!addedClientToRedis) {
        redis.sadd("allclients", clientID, function(err, resp) {
          //TODO: remove client upon shutdown?
          if (err) console.log(err);
          else addedClientToRedis = true
        });
      }
    }
    storing = false;
  });
}

var logLatency = function(host, elapsed_time) {
  if (optimizationMetric === 'avg') {
    //average, just keep running count
    if (sketch[host] === undefined) {
      sketch[host] = {
        cnt: 0,
        sum: 0.0,
      }
    }
    sketch[host].cnt += 1
    sketch[host].sum += elapsed_time
  } else {
    //quantile, use sketch
    if(sketch[host] === undefined) {
      sketch[host] = {
        cnt: 0,
        ddsketch: new DDSketch({alpha: 0.02}) // compute quantiles with precision of 2 percent
    }
    sketch[host].cnt += 1
    sketch[host].ddsketch.add(elapsed_time)
  }
}

var getRequestLatency = function(host) {
  if(optimizationMetric === 'avg') {
    return sketch[host].sum / sketch[host].cnt;
  } else {
    //just 99%ile for now
    return sketch[host].ddsketch.quantile(0.99);
  }
}

module.exports.logRequest = function(cli, cmd, key, elapsed, RedisClustr) {
  redis = RedisClustr
  //only accept sets and gets (for now...)
  if (cmd != "set" && cmd != "get") return;
  
  console.info(cli.address + " " + cmd + ' took %ds %dms', elapsed[0], elapsed[1] / 1000000)

  //log slot request
  var slot = calculateSlot(key);
  sketch.slots[slot] = (sketch.slots[slot] || 0) + 1;

  //log network request
  logLatency(cli.address, elapsed[0]*1000 + elapsed[1] / 1000000);

  //check if should dump to redis
  if (!lastStore) lastStore = process.hrtime();
  var sketchDumpPeriod = 10 //number of seconds to dump
  if (process.hrtime(lastStore)[0] > sketchDumpPeriod) {
    //it's been sketchDumpPeriod seconds, dump.
    storeSketch()
  }
};


