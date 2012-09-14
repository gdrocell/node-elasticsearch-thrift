var uuid = require('node-uuid');

/**
 * Round robin for elasticsearch connections
 */
function RoundRobinConnections() {

	this.current = this.first = this.last = null;
	this.connections = {};
}

/**
 * Allows to add new connection which is writable(that is which can be made to execute requests)
 * @param {Object} connection
 */
RoundRobinConnections.prototype.add = function(connection) {

	if(this.connections[connection.__roundRobinID]) {
		return;
	}

	if(typeof connection.connection.connection === 'undefined') { //Connection madness :)
		throw new Error('Missing connection field');
	}

	if(typeof connection.connection.connection.writable === 'undefined') {
		throw new Error('Missing writable property for conenction');
	}

	if(!connection.connection.connection.writable) {
		return;
	}

	var id = connection.__roundRobinID = uuid.v4();

	this.connections[id] = connection;

	if(this.first === this.last && this.first === null) {

		this.first = this.last = this.current = connection;
		connection.siblings = {
			next: connection,
			previous: connection
		};
	} else {

		connection.siblings = {
			next: this.first,
			previous: this.last
		};

		this.last.siblings.next = connection;
		this.first.siblings.previous = connection;
		this.last = connection;
	}
};

/**
 * Removes connection from round robin list
 * Used when connection gets closed
 *
 * @param  {Object} connection
 * @return {void}
 */
RoundRobinConnections.prototype.remove = function(connection) {

	if(!this.connections[connection.__roundRobinID]) {
		return;
	}

	var listConnection = this.connections[connection.__roundRobinID];

	//removing sole instancec of round robin list means rearanging all pointers
	if(this.first === connection && this.last === connection) {

		this.last = this.first = this.current = null;
		delete this.connections[connection.__roundRobinID];
	} else {
		listConnection.siblings.previous.siblings.next = connection.siblings.next;
		listConnection.siblings.next.siblings.previous = connection.siblings.previous;

		if(this.first === listConnection) {
			this.first = listConnection.siblings.next;
		}

		if(this.last === listConnection) {
			this.last = listConnection.siblings.previous;
		}

		if(this.current === listConnection) {
			this.current = listConnection.siblings.next;
		}
		delete this.connections[connection.__roundRobinID];
	}
};

/**
 * Returns next conenction from list which can be
 *
 * @return {Object}
 */
RoundRobinConnections.prototype.getNext = function() {

	var current,
		starting = this.current;

	do {
		current = this.current;
		this.current = current.siblings.next;

		if(current.connection.connection.writable) {
			return current;
		}
	} while(starting !== this.current);

	throw new Error('No servers connect to');
};

module.exports = RoundRobinConnections;