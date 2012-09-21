var thrift = require('thrift'),
	RoundRobinConnections = require('./lib/round.robin.connections'),
	ElasticRest = require('./lib/thrift/Rest'),
	elasticMethod = require('./lib/thrift/elasticsearch_types').Method,
	elasticStatus = require('./lib/thrift/elasticsearch_types').Status,
	ElasticRestReq = require('./lib/thrift/elasticsearch_types').RestRequest,
	uid = require('node-uuid');

/**
 * Helper method for checking whether whether in object there are set required properties
 *
 * If any of the fields is missing the Error is thrown
 *
 * @param  {Object} where    Object whos properties will be checked
 * @param  {Array}  required List with properties which must exist
 * @throws {Error}
 */
function requireOptions(where, required) {
	required.forEach(function (field) {
		if (!(field in where)) {
			throw new Error(field + ' must be specified');
		}
	});
}

/**
 * ElasticSearch wrapper which uses thrift as transport layer
 *
 * @param {Object}   options  Option object, currently the only possible and required option
 * is `servers` which specifies list with servers to connect to
 * @param {Function} callback Callback which will be called when all the servers have been connected
 */
function ElasticSearchThrift(options, callback) {

	requireOptions(options, ['servers']);

	if (!Array.isArray(options.servers) || !options.servers.length) {
		throw new Error('You must specify atleast one server');
	}

	this.options = options;
	this.servers = options.servers;
	this.roundRobin = new RoundRobinConnections();

	this.ready = false; //Flag whether all the servers have connected

	//We cache issued requests while servers haven't been connected yet
	this.pendingRequest = [];

	this.connectServers(callback);
}

/**
 * Checks whether all the connections has been established
 *
 * @param {Function} callback [description]
 */
ElasticSearchThrift.prototype.checkServerStartup = function (callback) {

	if (!this.connectedServerCnt && !this.totalServers) {
		throw new Error('COULDN\'T CONNECT TO ANY OF THE SERVERS');
	} else {
		this.lastUsedServer = this.totalServers - 1;
		this.ready = true;
		callback();
		this.executePendingRequests();
	}
};

ElasticSearchThrift.prototype.executePendingRequests = function () {

	var self = this;

	if (this.pendingRequest.length) {
		this.pendingRequest.forEach(function (request) {
			self.execute(request.request, request.callback);
		});
	}
};

/**
 * Connects to the specified servers
 *
 * @param {Function} callback
 */
ElasticSearchThrift.prototype.connectServers = function (callback) {

	var self = this,
		ready = false,
		readyCnt = 0;

	function callCallback() {

		if(self.ready) {
			return;
		}

		readyCnt += 1;

		if(self.options.readyWithOne) { //Allow to start working even with only one server connected
			self.ready = true;
			callback();
		} else {
			if(self.servers.length === readyCnt) {
				self.ready = true;
				callback();
			}
		}
	}

	this.servers.forEach(function (server) {

		self.createConnection(server, callCallback)
	});
};

/**
 * Creates new connection to the elasticsearch
 *
 * @return {void}
 */
ElasticSearchThrift.prototype.createConnection = function(server, callback) {
	//serverUid is used to identify failed connection when error happens
	var self = this;


	var connection = thrift.createConnection(server.host, server.port),
		client = thrift.createClient(ElasticRest, connection);

	connection.on('error', function () {
		self.createConnection(server, self.options.readyWithOne ? callback : null);
	});

	connection.on('connect', function () {

		self.roundRobin.add({
			client: client,
			connection: connection
		});

		if(callback) {
			callback();
		}
	});
};

/**
 * Executes arbitary request to ElasticSearch endpoint
 *
 * @param  {Object}   params
 * @param  {Function} callback
 */
ElasticSearchThrift.prototype.execute = function (params, callback) {

	requireOptions(params, ['uri', 'method']);

	var client,
		request = new ElasticRestReq({
			method: params.method,
			uri: params.uri,
			parameters: params.parameters,
			headers: params.headers,
			body: params.boy
		});

	if (this.ready) {
		try {
			client = this.roundRobin.getNext().client;
		} catch(e) {
			return callback(e);
		}

		client.execute(request, function(error, result) {

			var responseObject;

			if(error) {
				return callback(error);
			}

			if(typeof result === 'string') {
				return callback(new Error(result));
			}

			if(result.status >= 400) {
				return callback(new Error(result.body))
			}

			try {
				return callback(JSON.parse(result.body));
			} catch (e) {
				return callback(new Error(result.body));
			}
		});
	} else {
		this.pendingRequest.push({
			request: request,
			callback: callback
		});
	}
};

/**
 * Allows to execute GET request
 *
 * @param  {Object}   params
 * @param  {Function} callback
 * @return {[type]}
 */
ElasticSearchThrift.prototype.get = function (params, callback) {
	params.method = elasticMethod.GET;
	this.execute(params, callback);
};

/**
 * Allows to execute PUT request
 *
 * @param  {Object}   params
 * @param  {Function} callback
 */
ElasticSearchThrift.prototype.put = function (params, callback) {
	params.method = elasticMethod.PUT;
	this.execute(params, callback);
};

/**
 * Allows to execute post request
 *
 * @param  {Object}   params
 * @param  {Function} callback
 */
ElasticSearchThrift.prototype.post = function (params, callback) {
	params.method = elasticMethod.POST;
	this.execute(params, callback);
};

/**
 * Allows to execute delete request
 *
 * @param  {Object}   params
 * @param  {Function} callback
 */
ElasticSearchThrift.prototype.delete = function (params, callback) {
	params.method = elasticMethod.DELETE;
	this.execute(params, callback);
};

/**
 * Allows to execute head request
 *
 * @param  {Object}   params
 * @param  {Function} callback
 */
ElasticSearchThrift.prototype.head = function (params, callback) {
	params.method = elasticMethod.HEAD;
	this.execute(params, callback);
};

/**
 * Allows to execute options request
 *
 * @param  {Object}   params
 * @param  {Function} callback
 */
ElasticSearchThrift.prototype.options = function (params, callback) {
	params.method = elasticMethod.OPTIONS;
	this.execute(params, callback);
};

/**
 * Close all the open connections(as thrift is using net and end issued than only
 * half close happens, that is fin package is sent but server still can still send
 * answer to previous requests);
 */
ElasticSearchThrift.prototype.closeConnections = function () {
	var self = this;
	this.ready = false;
	this.connectionUidList.forEach(function (uid) {
		self.connections[uid].connection.end();
	});
};

module.exports = ElasticSearchThrift;