var calculateSlot = require('cluster-key-slot');

var sketch = {}

module.exports.dumpLog = function(cmd, elapsed) {
  Object.keys(sketch).forEach(function(node) {
    Object.keys(sketch[node]).forEach(function(cmd) {
      var sum = sketch[node][cmd].sum
      var cnt = sketch[node][cmd].cnt
      var keys = sketch[node][cmd].keys
      console.log("Accessed", node, cmd, cnt, "times. Took", sum / cnt, "ms on average...");
      console.log(keys);
    });
  });
};

module.exports.logRequest = function(cli, cmd, key, elapsed) {
  //only accept sets and gets (for now...)
  if (cmd != "set" && cmd != "get") return;
  
  console.info(cli.address + " " + cmd + ' took %ds %dms', elapsed[0], elapsed[1] / 1000000)
  if (sketch[cli.address] === undefined) {
    sketch[cli.address] = {}
  }
  if (sketch[cli.address][cmd] === undefined) {
    sketch[cli.address][cmd] = {
      cnt: 0,
      sum: 0.0,
      keys: {}
    }
  }
  sketch[cli.address][cmd].cnt += 1
  sketch[cli.address][cmd].sum += elapsed[0]*1000 + elapsed[1] / 1000000
  if (sketch[cli.address][cmd].keys[key] === undefined) {
    sketch[cli.address][cmd].keys[key] = 1
  } else {
    sketch[cli.address][cmd].keys[key] += 1
  }
};
