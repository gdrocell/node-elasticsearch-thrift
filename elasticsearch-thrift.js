var thrift = require('thrift'),
	ElasticRest = require('./lib/Rest'),
	elasticMethod = require('./lib/elasticsearch_types').Method,
	elasticStatus = require('./lib/elasticsearch_types').Status,
	ElasticRestReq = require('./lib/elasticsearch_types').RestRequest,
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

	this.servers = options.servers;
	this.connections = {}; //Here goes the connections after they have been established
	this.connectionUidList = []; //Each conenction gets unique uid, this array contains all of them

	this.totalServers = this.servers.length; //Total count of servers to which we must connect

	//Counter for checking whether we have connected to all the servers and we can invoke callback
	this.connectedServerCnt = 0;
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
		this.executePendingRequests.forEach(function (request) {
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

	var self = this;

	this.servers.forEach(function (server) {

		//serverUid is used to identify failed connection when error happens
		var serverUid = uid.v1();

		var connection = thrift.createConnection(server.host, server.port),
			client = thrift.createClient(ElasticRest, connection);

		connection.on('error', function () {

			if (self.connections[serverUid]) { //Error has happened to already connected server
				//@TODO what to do now?
			} else { //Error happened during connection so we skip this server
				self.totalServers -= 1;
			}

			self.checkServerStartup(callback);
		});

		//When connection is established succesfuly then it can be added to connection pool
		connection.on('connect', function () {

			self.connectionUidList.push(serverUid);

			self.connections[serverUid] = {
				connection: connection,
				client: client,
				parameters: server
			};

			self.connectedServerCnt += 1;

			self.checkServerStartup(callback);
		});
	});
};

/**
 * Round Robin so that we would use each of the servers equaly ofter
 */
ElasticSearchThrift.prototype.roundRobinServer = function () {

	this.lastUsedServer = ++this.lastUsedServer % this.totalServers;
	return this.connections[this.connectionUidList[this.lastUsedServer]].client;
};

/**
 * Executes arbitary request to ElasticSearch endpoint
 *
 * @param  {Object}   params
 * @param  {Function} callback
 */
ElasticSearchThrift.prototype.execute = function (params, callback) {

	requireOptions(params, ['uri', 'method']);

	var request = new ElasticRestReq({
			method: params.method,
			uri: params.uri,
			parameters: params.parameters,
			headers: params.headers,
			body: params.boy
		});

	if (this.ready) {
		var client = this.roundRobinServer();
		client.execute(request, callback);
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
	this.execute(params, function (err, results) {
		//Handle thrift error
		if (err) {
			return callback(err);
		}

		if (results.status >= 400) {
			return callback(new Error('ElasticSearch error ' + results.body));
		}

		callback(err, results);
	});
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