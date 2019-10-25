var calculateSlot = require('cluster-key-slot');

var sketch = {slots:{}};
var lastStore = undefined;
var redis = undefined;
var addedClientToRedis = false;

//NOTE: THIS ONLY SUPPORTS CONNECTIONS TO ONE REDIS INSTANCE AT A TIME...
//TODO: Should only sketch one window at a time???

module.exports.printLog = function(cmd, elapsed) {
  Object.keys(sketch).forEach(function(node) {
    var sum = sketch[node].sum
    var cnt = sketch[node].cnt
    console.log("Accessed", node, cmd, cnt, "times. Took", sum / cnt, "ms on average...");
  });
  console.log("Accessed ", Object.keys(sketch.slots).length, " slots:", Object.keys(sketch.slots));
};

var storing = false;
var clientID = require('uuid/v1')();
var storeSketch = function() {
  //check if you're already in the middle of doing this...
  if(storing) return;
  storing = true;

  sketch['time'] = Date.now()

  //store sketch
  redis.setex(clientID, 600, JSON.stringify(sketch), function(err, resp) {
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

module.exports.logRequest = function(cli, cmd, key, elapsed, RedisClustr) {
  redis = RedisClustr
  //only accept sets and gets (for now...)
  if (cmd != "set" && cmd != "get") return;
  
  console.info(cli.address + " " + cmd + ' took %ds %dms', elapsed[0], elapsed[1] / 1000000)
  if (sketch[cli.address] === undefined) {
    sketch[cli.address] = {
      cnt: 0,
      sum: 0.0,
    }
  }

  var slot = calculateSlot(key);
  sketch.slots[slot] = (sketch.slots[slot] || 0) + 1;

  //hardset to all for now
  sketch[cli.address].cnt += 1
  sketch[cli.address].sum += elapsed[0]*1000 + elapsed[1] / 1000000

  //check if i should dump to redis
  if (!lastStore) lastStore = process.hrtime();
  var sketchDumpPeriod = 10 //number of seconds to dump
  if (process.hrtime(lastStore)[0] > sketchDumpPeriod) {
    //it's been sketchDumpPeriod seconds, dump.
    storeSketch()
  }
};


