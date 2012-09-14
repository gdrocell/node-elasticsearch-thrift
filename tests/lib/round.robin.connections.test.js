var should = require('should'),
	RoundRobinConnections = require('../../lib/round.robin.connections');

describe('round.robin.connections.js', function() {
	describe('RoundRobinConnections', function() {
		it('should be exported as function', function() {
			RoundRobinConnections.should.be.a('function');
		});

		it('should point every pointer to null', function() {
			var robin = new RoundRobinConnections();
			should.strictEqual(null, robin.first);
			should.strictEqual(null, robin.last);
			should.strictEqual(null, robin.current);
			robin.connections.should.be.eql({});
		});
	});

	describe('add', function() {
		var robin = new RoundRobinConnections(),
			con1 = {
				connection: {
					connection: {
						writable: true
					}
				}
			},
			con2 = {
				connection: {
					connection: {
						writable: true
					}
				}
			},
			con3 = {
				connection: {
					connection: {
						writable: true
					}
				}
			},
			con4 = {
				connection: {
					connection: {
						writable: false
					}
				}
			};

		it('shold point all the pointers to the added one as list was empty', function() {
			robin.add(con1);
			robin.first.should.be.equal(con1, 'first');
			robin.last.should.be.equal(con1, 'last');
			robin.current.should.be.eql(con1, 'current');
			Object.keys(robin.connections).length.should.be.equal(1);
		});

		it('should allow to add second one also', function() {
			robin.add(con2);
			robin.first.should.be.equal(con1, 'first');
			robin.current.should.be.equal(con1, 'current');
			robin.last.should.be.equal(con2, 'last');
			Object.keys(robin.connections).length.should.be.equal(2);
		});

		it('should not add connection if it is already there', function() {
			robin.add(con1);
			Object.keys(robin.connections).length.should.be.equal(2);
			robin.first.should.be.equal(con1, 'first');
			robin.current.should.be.equal(con1, 'current');
			robin.last.should.be.equal(con2, 'last');
		});

		it('should also add 3rd connection with correct links', function() {
			robin.add(con3);
			robin.last.should.be.equal(con3);
			con3.siblings.previous.should.be.equal(con2);
			con3.siblings.next.should.be.equal(con1);
			con2.siblings.previous.should.be.equal(con1);
			con2.siblings.next.should.be.equal(con3);
			con1.siblings.previous.should.be.equal(con3);
			con1.siblings.next.should.be.equal(con2);
		});

		it('should not to allow add non writable connection', function() {
			robin.add(con4);
			robin.last.should.be.equal(con3);
			Object.keys(robin.connections).length.should.be.equal(3);
		});
	});

	describe('remove', function() {
		var robin, con1, con2, con3;

		beforeEach(function() {
			robin = new RoundRobinConnections();
			con1 = {
				connection: {
					connection: {
						writable: true
					}
				}
			};
			con2 = {
				connection: {
					connection: {
						writable: true
					}
				}
			};
			con3 = {
				connection: {
					connection: {
						writable: true
					}
				}
			};
			robin.add(con1);
			robin.add(con2);
			robin.add(con3);
		});

		it('should allow removing the first element', function() {
			robin.remove(con1);

			Object.keys(robin.connections).length.should.be.equal(2);

			robin.first.should.be.equal(con2, 'first');
			robin.last.siblings.next.should.be.equal(con2, 'last next');
			robin.first.siblings.previous.should.be.equal(con3, 'first previous');
		});

		it('should allow removing the last element', function() {
			robin.remove(con3);

			Object.keys(robin.connections).length.should.be.equal(2);

			robin.last.should.be.equal(con2);
			robin.last.siblings.next.should.be.equal(con1);
			robin.first.siblings.previous.should.be.equal(con2);
		});

		it('should allow removing the middle element', function() {
			robin.remove(con2);

			Object.keys(robin.connections).length.should.be.equal(2);

			robin.last.should.be.equal(con3);
			robin.first.siblings.next.should.be.equal(con3);
			robin.last.siblings.previous.should.be.equal(con1);
		});

		it('should allow removing all instances', function() {
			robin.remove(con1);
			robin.remove(con2);
			robin.remove(con3);

			should.strictEqual(null, robin.last);
			should.strictEqual(null, robin.current);
			should.strictEqual(null, robin.first);

			Object.keys(robin.connections).length.should.be.equal(0);
			robin.connections.should.be.eql({});
		});
	});

	describe('getNext', function() {
		describe('without connections', function() {
			it('should throw an error if there are no connections', function() {
				(function() {
					new RoundRobinConnections().getNext();
				}).should.throw();
			});
		});

		describe('with connections', function() {
			var robin, con1, con2, con3;

			before(function() {
				robin = new RoundRobinConnections();
				con1 = {
					connection: {
						connection: {
							writable: true
						}
					}
				};
				con2 = {
					connection: {
						connection: {
							writable: true
						}
					}
				};
				con3 = {
					connection: {
						connection: {
							writable: true
						}
					}
				};
				robin.add(con1);
				robin.add(con2);
				robin.add(con3);
			});

			it('should return the first connection', function() {
				robin.getNext().should.be.equal(con1);
			});

			it('should return the second one now', function() {
				robin.getNext().should.be.equal(con2);
			});

			it('should return the third one', function() {
				robin.getNext().should.be.equal(con3);
			});

			it('should now return the first one as list has looped to the first one', function() {
				robin.getNext().should.be.equal(con1);
			});
		});

		describe('with non writable connections', function() {
			var robin, con1, con2, con3;

			before(function() {

				robin = new RoundRobinConnections();

				con1 = {
					connection: {
						connection: {
							writable: true
						}
					}
				};
				con2 = {
					connection: {
						connection: {
							writable: true
						}
					}
				};
				con3 = {
					connection: {
						connection: {
							writable: true
						}
					}
				};

				robin.add(con1);
				robin.add(con2);
				robin.add(con3);

				con1.connection.connection.writable = false;
				con2.connection.connection.writable = false;
				con3.connection.connection.writable = false;
			})

			it('should fail with error', function() {
				(function() {
					robin.getNext();
				}).should.throw();
			})
		})
	});
});
