#!/usr/bin/env node-fibers

var sluice = require('./sluice');
var println = console.log;

function Counter(limit)
{
    this.n = limit;
    this.cnt = 0;
    this.reset = function() {
	this.cnt = 0;
    };
    this.work = function() {
	if (this.cnt >= this.n) return true;
	this.push(++this.cnt);
	return false;
    };
}

function Adder(arg)
{
    this.n = arg;
    this.work = function() {
	this.push(this.pop()+this.n);
	return false;
    };
}

function Printer()
{
    this.work = function() {
	println(this.pop());
	return false;
    };
}

var cnt = new Counter(10);
var sj = sluice.SplitRR(2, new Adder(1), new Adder(100), new Adder(1000)).JoinRR(1);
var p = sluice.Pipeline(cnt, sj, new Printer());

p.run();
cnt.reset();
p.run();

println("");println("");

cnt = new Counter(5);
sj = sluice.SplitDup(2, new Adder(-1), new Adder(-100), new Adder(-1000)).JoinRR(10);
p = sluice.Pipeline(cnt, sj, new Printer());

p.run();
