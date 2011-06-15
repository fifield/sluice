
//
// sluice.js - streamit like concurrency for node
//

require('fibers');

var createStream = createArrayStream;
var createPop = createArrayPop;
var createPush = createArrayPush;

exports.Pipeline = createPipeline;
exports.SplitRR = createSplitRR;
exports.SplitDup = createSplitDup;

exports.BufferSource = BufferSource;
exports.BufferSink = BufferSink;

var streamId = 0;
function createArrayStream(callback) {
    var s = new function() {
	this.id = streamId++;
	this.data = new Array();
	this.src = -1;
	this.dst = -1;
	this.done = function(which) {
	    if (which == 'src') {
		this.src = 1;
	    } else if (which == 'dst') {
		this.dst = 1;
	    } else throw 'bad argument';
	};
	this.reset = function(which) {
	    if (which == 'src') {
		this.src = -1;
	    } else if (which == 'dst') {
		this.dst = -1;
	    } else throw 'bad argument';
	};
    };
    return s;
};

// return a pop operation
function createArrayPop(kernel,stream) {
    var k = kernel;
    if (stream != null) {
	var s = stream;
	return function() {
	    var a = k.ins[s].data;
	    while (!a.length) yield(k.ins[s].src == 1);
	    var e = a.shift();
	    return e;
	};
    } else {
	return function(s) {
	    var a = k.ins[s].data;
	    while (!a.length) yield(k.ins[s].src == 1);
	    var e = a.shift();
	    return e;
	};
    }
}

// return a push operation
// 5000 is fairly arbitrary here...
function createArrayPush(kernel,stream) {
    var k = kernel;
    if (stream != null) {
	var s = stream;
	return function(data) {
	    var a = k.outs[s].data;
	    if (a.length > 5000) yield(k.outs[s].dst == 1);
	    k.outs[s].data.push(data);
	};
    } else {
	return function(s, data) 
	{
	    var a = k.outs[s].data;
	    if (a.length > 5000) yield(k.outs[s].dst == 1);
	    k.outs[s].data.push(data);
	};
    }
}

// extend kernel work function
function createKernel(workfn) 
{
    var that = workfn;

    if (that._sluice_kernel !== true) {
	that._sluice_kernel = true;

	if (that.work == undefined)
	    that.work = workfn;

	that.ins = [];
	that.outs = [];

	that.pop = createPop(that,0);
	that.popN = createPop(that);
	that.push = createPush(that,0);
	that.pushN = createPush(that);

	that.fiber = Fiber( 
	    function() {
		var r = false;
		while(r == false)
		    r = that.work.call(that);
		return r;
	    });

    }
    return that;
}

// generate scheduling work function
function FiberSched(k) 
{
    this.kernels = k.concat();
    this.saved_kernels = k.concat();

    this.run = function(cb) {

	while (this.kernels.length) {
	    for (var i=0; i<this.kernels.length; i++) {
		
		var r = this.kernels[i].fiber.run();
		if (r == true) {
		    for (var j = 0; j<this.kernels[i].outs.length; j++)
			this.kernels[i].outs[j].done('src');
		    this.kernels.splice(i,1);
		}
	    }

	    // without this, it's synchronous
	    if (cb && this.kernels.length) {
		setTimeout(function(_k, _cb) { _k.run.call(_k, _cb); }, 0, this, cb);
		return false;
	    }
	}

	// reset the kernel list and the streams it contains
	for (var i=0; i<this.saved_kernels.length; i++) {
	    for (var j = 0; j<this.saved_kernels[i].outs.length; j++)
		this.saved_kernels[i].outs[j].reset('src');// = -1;
	    for (var j = 0; j<this.saved_kernels[i].ins.length; j++)
		this.saved_kernels[i].ins[j].reset('dst');// = -1;
	}
	this.kernels = this.saved_kernels.concat();
	if (cb) cb();
	return true;
    };
}

// flatten any arrays in args
function processArgs(args)
{
    var ret = [];
    for (var i=0; i<args.length; i++) {
	var a = args[i];
	if (a instanceof Array)
	    ret = ret.concat(processArgs(a));
	else
	    ret.push(a);
    }
    return ret;
}

// create a pipeline of kernels
function createPipeline()
{
    var inStream = null;
    var outStream = null;
    var kernels = [];
    var in_k, out_k;
    
    var args = processArgs(arguments);

    for (var i=0; i<args.length; i++) {
	var work = args[i];
	if (work.kernels) {
	    in_k = work.kernels[0];
	    out_k = work.kernels[work.kernels.length-1];
	    kernels = kernels.concat(work.kernels);
	} else {
	    var k = createKernel(work);
	    in_k = out_k = k;
	    kernels.push(k);
	}
	
 	if (inStream) {
	    in_k.ins.push(inStream);
	}
	if ((i+1) < args.length) {
	    outStream = createStream();
	    out_k.outs.push(outStream);
	    inStream = outStream;
	}

    }
    
    var k = new FiberSched(kernels);
    return k;
}

// return a custom joiner constructor
// 'split' is the split scope
// 'ins' are the kernels (streams) being joined
function createJoiner(splt, inks)
{
    var split = splt;
    var ins = inks;
    return function(arg) {
	// create the join workfn
	var join = function(a, n) {
	    this.a = a;
	    this.n = n;
	    if (this.a > 1) {
		this.work = function() {
		    for (var i=0; i<this.n; i++)
			for (var j=0; j<this.a; j++)
			    this.push(this.popN(i));
		    return false;
		};
	    } else {
		this.work = function() {
		    for (var i=0; i<this.n; i++)
			this.push(this.popN(i));
		    return false;
		};
	    }
	};
	
	join = createKernel(new join(arg, ins.length));
	split.kernels.push(join);
	
	// inputs to the join
	for (var i=0; i<ins.length; i++) {   
	    var in_k = ins[i];
	    var s = createStream();
	    in_k.outs.push(s);
	    join.ins.push(s);
	}
	
	return split;
    };
};

// create a split_rr kernel
function split_rr (a, n) {
    this.a = a;
    this.n = n;
    if (this.a > 1) {
	this.work = function() {
	    for (var i=0; i<this.n; i++)
		for (var j=0; j<this.a; j++)
		    this.pushN(i, this.pop());
	    return false;
	};
    } else {
	this.work = function() {
	    for (var i=0; i<this.n; i++)
		this.pushN(i, this.pop());
	    return false;
	};
    }
}

// create a split_dup kernel
function split_dup(a, n) {
    this.a = a;
    this.n = n;
    if (this.a > 1)
	this.work = function() {
	    var e = this.pop();
	    for (var i=0; i<this.n; i++) {
		for (var j=0; j<this.a; j++)
		    this.pushN(i, e);
	    }
	    return false;
	};
    else
	this.work = function() {
	    var e = this.pop();
	    for (var i=0; i<this.n; i++) {
		this.pushN(i, e);
	    }
	    return false;
	};
}

function createSplit (splitfn, arg, outs)
{
    var kernels = [];
    var nouts = outs.length;
    
    var join_kernels = [];
    for (var i=0; i<nouts; i++) {
	var k = outs[i];
	if (k.kernels)
	    k = k.kernels[k.kernels.length-1];
	join_kernels.push(k);
    }
    
    var split = new splitfn(arg, nouts);
    split = createKernel(split);
    kernels.push(split);

    for (var i=0; i<nouts; i++) {
	var in_k = outs[i];
	
	if (in_k.kernels) {
	    kernels = kernels.concat(in_k.kernels);
	    in_k = in_k.kernels[0];
	} else {
	    in_k = createKernel(in_k);
	    kernels.push(in_k);
	}
	    
	var s = createStream();
	split.outs.push(s);
	in_k.ins.push(s);
    }
	
    var k = new FiberSched(kernels);
    k.JoinRR = createJoiner(k, join_kernels);

    return k;
}

function createSplitRR (arg) {
    var outs = processArgs(Array.prototype.slice.call(arguments, 1));
    return createSplit(split_rr, arg, outs);
}

function createSplitDup (arg) {
    var outs = processArgs(Array.prototype.slice.call(arguments, 1));
    return createSplit(split_dup, arg, outs);
}

function BufferSource (buf) {
    this.b = buf;
    this.i = 0;
    this.work = function () {
	if (this.i >= this.b.length)
	    return true;
	this.push(this.b[this.i]);
	this.i++;
	return false;
    };
}

function BufferSink (buf) {
    this.b = buf;
    this.i = 0;
    this.work = function () {
	if (this.i >= this.b.length)
	    return true;
	var e = this.pop();
	this.b[this.i] = e;
	this.i++;
	return false;
    };
}
