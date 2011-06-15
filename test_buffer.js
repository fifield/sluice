#!/usr/bin/env node-fibers

var sluice = require('./sluice');

function Adder(arg)
{
    this.n = arg;
    this.work = function() {
	this.push(this.pop()+this.n);
	return false;
    };
}

var b = new Buffer(10);
var s = new Buffer(10);

for (var i=0; i<b.length; i++)
{
    b[i] = i;
}

var p = sluice.Pipeline(new sluice.BufferSource(b), 
			new Adder(10),
			new sluice.BufferSink(s)).run(
			    function() {
				for (var i=0; i<s.length; i++) {
				    console.log(s[i]);
				}
			    }
			);