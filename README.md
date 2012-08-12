# ElasticSearch thrift

Basic Elasticsearch thrift transport protocol wrapper

## Install
	npm install elasticsearch-thrift

## Usage

	var search,
	ElasticSearch = require('../elasticsearch-thrift');

	search = new ElasticSearch({
		servers: [{
			host: 'localhost',
			port: 9500
		}]}, function() {
		console.log('connected');

		search.closeConnections();

		search.get({uri: '_stats'}, function(err, res){
			console.log(err, res);
			search.closeConnections();
		});
	});

## Elasticsearch configuration

To use Elasticsearch with thrift first you must install thrift plugin by executing :

	bin/plugin -install elasticsearch/elasticsearch-transport-thrift/1.2.0

And set few configuration options

	thrift:
		port: 9500 # any of your choice
		frame: 1mb # node thrift module currently only supports framed transport protocol

Thrift file is taken from official Elasticsearch thrift transport [repo schema](https://github.com/elasticsearch/elasticsearch-transport-thrift/blob/master/elasticsearch.thrift)