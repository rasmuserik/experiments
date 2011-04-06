var fs = require("fs");

function linereader(fname, linehandle, endhandle) {
    var prev = "";

    fs.createReadStream(fname, {encoding: "utf-8"}).on("data", function(data) {
        data = prev + data;
        data = data.split("\n");
        for(var i=0;i<data.length - 1; ++i) {
            linehandle(data[i]);
        }
        prev = data[i];
    }).on("end", function() {
        if(prev.length > 0) linehandle(prev);
        endhandle();
    }).on("error", function(e) {
        throw e;
    })
}


count = 0;
hits = 0;
data = [];
linereader('pages.en.hits', function(line) {
    line = line.split(" ");
    data.push([line[0], hits, +line[1]]);
    hits += +line[1];
    count++;
}, function() {
    console.log("read #lines", count, "with", hits, "hits");
    console.log("normalising data");
    data = data.map(function(datum) {
        return {name: datum[0], pos: datum[1]/hits, prob: datum[2]/hits};
    });

    console.log("data normalised");

    var maxdepth = 5;
    for(var i = 0; i < maxdepth;++i) {
        console.log("extracting level ", i);

        function heapput(heap, elem) {
            if(!heap) {
                heap = [];
            }
            if(heap.length < 2) {
                heap.push(elem);
            } else {
                for(var i = 0; i < 2; ++i) {
                    if(heap[i].prob < elem.prob) {
                        var t = heap[i];
                        heap[i] = elem;
                        elem = t;
                    }
                }
            }
            return heap;
        }

        var result = undefined;
        base16 = "0123456789abcdef"
        function writeFile() {
            if(result) {
                var name = ".js";
                var t = prev_id;
                for(var n =0; n<i;++n) {
                    name = base16[15&t] + name;
                    t/=16;
                }
                name = "_" + name;

                // save result to file;
                var out = [];
                result.map(function(x) {
                    x.map(function(y) {
                        out.push(y);
                    });
                });
                out.sort(function(a, b) { return a.pos - b.pos; });
                if(out.length>0) {
                    fs.writeFileSync(name, "__jsonp_callback(" + JSON.stringify(out) + ");");
                }
                //console.log("HERE", bucket_id, name, JSON.stringify(out));
            }
       }
        var bucket_count = 1 <<4*i;
        var prev_id = -1;
        for(var j = 0; j < data.length; ++j) {
            var bucket_id = 0 | j*bucket_count/data.length;
            var subbucket_id = 0|(j*bucket_count*16/data.length) % bucket_count;

            if(prev_id !== bucket_id) {
                writeFile();
                result = [];
                result[0] = [];
            }
            prev_id = bucket_id;

            if(i < maxdepth-1) {
                result[subbucket_id] = heapput(result[subbucket_id], data[j]);
            } else {
                result[0].push(data[j]);
            }

            if(j % (0|data.length / 100) === 0) { console.log((0|100*j/data.length) + "%") };
        }
        writeFile();

    }
    console.log("data treed");
});
