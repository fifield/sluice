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
var add = new Adder(1);
var pnt = new Printer();

var p = sluice.Pipeline(cnt, add, pnt);
p.run();

cnt.reset();
p.run();

var sub2 = new Adder(-2);
var add2 = new Adder(2);
p = sluice.Pipeline(cnt, add, sluice.Pipeline(sub2, add2), pnt);

cnt.reset();
p.run();
