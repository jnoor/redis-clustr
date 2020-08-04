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

var enabled = true;
module.exports.setEnabled = function(enble) {
  enabled = enble;
}

module.exports.printLog = function() {
  var totalsum = 0;
  var totalcnt = 0;
  Object.keys(sketch).forEach(function(node) {
    var cnt = sketch[node].cnt
    var latency = getRequestLatency(node);
    console.log("Accessed", node, cnt, "times. Took", latency, "ms...");
    if(latency && cnt && cnt > 2) {
      totalsum += latency * cnt;
      totalcnt += cnt;
    }
  });
  console.log("Accessed ", Object.keys(sketch.slots).length, " slots");//, Object.keys(sketch.slots));
  console.log("Overview: total of ", totalsum, "ms with", totalcnt, "requests");
  console.log("Overview: avg latency:", totalsum/totalcnt, "ms");
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

var clientID = require('uuid/v1')();
module.exports.storeSketch = function(callback) {
  //check if you're already in the middle of doing this...
  if (!enabled) return callback("Sketching disabled...");
  sketch['time'] = Date.now()

  //store sketch
  redis.setex(clientID, 6000, getStorableSketch(), function(err, resp) {
    if (err) return callback(err);
    else {
      console.log("set sketch into redis.");
      lastStore = process.hrtime();

      //ensure client is stored in the client-key
      if (!addedClientToRedis) {
        redis.sadd("allclients", clientID, function(err, resp) {
          //TODO: remove client upon shutdown?
          if (err) return callback(err);
          else addedClientToRedis = true;
          return callback(null);
        });
      } else {
        return callback(null);
      }
    }
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
    }
    sketch[host].cnt += 1
    sketch[host].ddsketch.add(elapsed_time)
  }
}

var getRequestLatency = function(host) {
  if(optimizationMetric === 'avg' || sketch[host].ddsketch === undefined) {
    return sketch[host].sum / sketch[host].cnt;
  } else {
    //just 99%ile for now
    return sketch[host].ddsketch.quantile(0.99);
  }
}

module.exports.logRequest = function(cli, cmd, key, elapsed, RedisClustr) {
  redis = RedisClustr
  //only accept sets and gets (for now...)
  if (cmd != "set" && cmd != "get" && cmd != "hmset" && cmd != "hgetall") return;
  
  console.info(cli.address + " " + cmd + ' took %ds %dms', elapsed[0], elapsed[1] / 1000000)

  //log slot request
  var slot = calculateSlot(key);
  sketch.slots[slot] = (sketch.slots[slot] || 0) + 1;

  //log network request
  logLatency(cli.address, elapsed[0]*1000 + elapsed[1] / 1000000);

  //check if should dump to redis
  if (!lastStore) lastStore = process.hrtime();
  var sketchDumpPeriod = 10 //number of seconds to dump
  if (enabled && process.hrtime(lastStore)[0] > sketchDumpPeriod) {
    //it's been sketchDumpPeriod seconds, dump.
    module.exports.storeSketch(function(err) {
      if(err) console.log("ERROR", err);
    })
  }
};


