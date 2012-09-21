var ElasticSearchThrift = require('../elasticsearch-thrift'),
	net = require('net'),
	should = require('should');

describe('elasticsearch-thrift.js', function() {
	describe('ElasticSearchThrift', function() {
		it('should be exported as function', function() {
			ElasticSearchThrift.should.be.a('function');
		});

		it('should throw an error if there are no servers in the options', function() {
			(function() {
				new ElasticSearchThrift({});
			}).should.throw('servers must be specified');
		});

		it('should thow an error if the server property is not an array', function() {
			(function() {
				new ElasticSearchThrift({servers: false});
			}).should.throw('You must specify atleast one server');
		});

		it('should throw an error if the server list is empty', function() {
			(function() {
				new ElasticSearchThrift({servers: []});
			}).should.throw('You must specify atleast one server');
		});

		describe('calling callback', function() {
			var server, server2, cons = [];

			beforeEach(function() {
				server = net.createServer(function(c){cons.push(c);}).listen(19500);
				server2 = net.createServer(function(c){cons.push(c);}).listen(19501);
			});

			afterEach(function(done) {

				var called = 0,
					ready = function() {
						called += 1;
						if(called === 2) {
							done();
						}
					};

				cons.forEach(function(c) {
					c.end();
				});

				server.close(ready);
				server2.close(ready);
			});

			it('should execute callback when one the server has connected', function(done) {
				var elastic,
					config = {
						servers: [{
								host: 'localhost',
								port: 19500
							}]
					};

				elastic = new ElasticSearchThrift(config, function() {
					Object.keys(elastic.roundRobin.connections).length.should.be.equal(1);
					done();
				});
			});

			it('should execute callback also with two servers and add them to round \
			robin', function(done) {
				var elastic,
					config = {
						servers: [
							{
								host: 'localhost',
								port: 19500
							},
							{
								host: 'localhost',
								port: 19501
							}
						]
					};

				elastic = new ElasticSearchThrift(config, function() {
					Object.keys(elastic.roundRobin.connections).length.should.be.equal(2);
					done();
				});
			});

			it('should allow to execute callback even if one of the servers is still down with the \
			readyWithOne flag and add that server to round robin', function(done) {

				var elastic,
					config = {
						servers: [
							{
								host: 'localhost',
								port: 19500
							},
							{
								host: 'localhost',
								port: 19666
							}
						],
						readyWithOne: true
					};

				elastic = new ElasticSearchThrift(config, function() {
					Object.keys(elastic.roundRobin.connections).length.should.be.equal(1);
					done();
				});
			});
		});
	});

	describe('connection listening', function() {

		var server, cons = [];

			beforeEach(function() {
				server = net.createServer(function(c){cons.push(c);}).listen(19500);
			});

			afterEach(function(done) {

				cons.forEach(function(con) {
					con.end();
				});

				server.close(done);
			});

		it('should add connection if server comes up', function(done) {
			var elastic,
				config = {
					servers: [
						{
							host: 'localhost',
							port: 19500
						},
						{
							host: 'localhost',
							port: 19501
						}
					],
					readyWithOne: true
				};

			elastic = new ElasticSearchThrift(config, function() {

				Object.keys(elastic.roundRobin.connections).length.should.be.equal(1);

				var server2 = net.createServer(function(c){cons.push(c);}).listen(19501);

				setTimeout(function() {
					Object.keys(elastic.roundRobin.connections).length.should.be.equal(2);
					server2.close();
					done();
				}, 1500);
			});
		});

		it('should remove connection if it goes down', function() {
			var elastic, cons2 = [],
				config = {
					servers: [
						{
							host: 'localhost',
							port: 19500
						},
						{
							host: 'localhost',
							port: 19501
						}
					]
				},
				server2 = net.createServer(function(c){
					cons.push(c);
					cons2.push(c);
				}).listen(19501);

			elastic = new ElasticSearchThrift(config, function() {
				Object.keys(elastic.roundRobin.connections).length.should.be.equal(2);
				cons2.forEach(function(con) {
					con.end();
				});

				server2.close(function() {
					Object.keys(elastic.roundRobin.connections).length.should.be.equal(1);
				});
			});
		});
	});
});