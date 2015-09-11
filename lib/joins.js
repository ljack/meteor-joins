var Mongo = Package.mongo.Mongo;

var globalContext = this;

Mongo.Collection.prototype.doJoin = function(collectionObject, collectionName, collectionNameField, foreignKey, containerField, fieldList) {
	this._joins = this._joins || [];

	this._joins.push({
		collectionObject: collectionObject,
		collectionName: collectionName,
		collectionNameField: collectionNameField,
		foreignKey: foreignKey,
		containerField: containerField,
		fieldList: fieldList
	});

	var __original = {
		find: Mongo.Collection.prototype.find,
		findOne: Mongo.Collection.prototype.findOne
	};

	this.findOne = function(selector, options) {
		var self = this;
		selector = selector || {};
		options = options || {};

		console.log("findOne selector=", selector, " options=", options);

		var originalTransform = options.transform || null;

		options.transform = function(doc) {
			_.each(self._joins, function(join) {
				var opt = {};
				if (join.fieldList && join.fieldList.length) {
					opt.fields = {};
					_.each(join.fieldList, function(field) {
						opt.fields[field] = 1;
					});
				}

				var coll = null;
				if (join.collectionObject)
					coll = join.collectionObject;
				else if (join.collectionName)
					coll = globalContext[join.collectionName];
				else if (join.collectionNameField)
					coll = globalContext[doc[join.collectionNameField]];

				if (coll) {
					var fk = doc[join.foreignKey];
					var data = __original.findOne.call(coll, {
						_id: fk
					}, opt);
					console.log("fk=", fk, " data=", data);
					var container = join.containerField || coll._name + "_joined";
					doc[container] = data;
				}
			});
			if (originalTransform)
				return originalTransform(doc);
			else
				return doc;
		};

		return __original.findOne.call(this, selector, options);
	};

	this.find = function(selector, options) {
		var self = this;
		selector = selector || {};
		options = options || {};

		console.log("find selector=", selector, " options=", options);

		var originalTransform = options.transform || null;

		options.transform = function(doc) {
			_.each(self._joins, function(join) {
				var opt = {};
				if (join.fieldList && join.fieldList.length) {
					opt.fields = {};
					_.each(join.fieldList, function(field) {
						opt.fields[field] = 1;
					});
				}

				var coll = null;
				if (join.collectionObject)
					coll = join.collectionObject;
				else if (join.collectionName)
					coll = globalContext[join.collectionName];
				else if (join.collectionNameField)
					coll = globalContext[doc[join.collectionNameField]];

				if (coll) {
					// HERE, add support for array of foreingKey
					// replace findOne with find
					// replace filter with $in { field: { $in: [<value1>, <value2>, ... <valueN> ] } }		

					//var data = __original.findOne.call(coll, { _id: doc[join.foreignKey] }, opt);
					// var data = __original.find.call(coll, { _id: { $in: doc[join.foreignKey] } } , opt);
					//[ doc[join.foreignKey] ]

					var fkId = doc[join.foreignKey];
					if (Array.isArray(fkId)) {
						console.log("isArray is true fk=",fkId);
						var data = __original.find.call(coll, {_id: {$in: fkId} }, opt).fetch();
						
						var container = join.containerField || coll._name + "_joined";
						doc[container] = data;
						console.log("doc=",doc);
					}
					else {
						
						console.log("pks=", fkId);
						var data = __original.findOne.call(coll, {_id: fkId }, opt);
						console.log("data=", data);
						var container = join.containerField || coll._name + "_joined";
						doc[container] = data;
					}
					
					


				}
			});
			if (originalTransform)
				return originalTransform(doc);
			else
				return doc;
		};

		return __original.find.call(this, selector, options);
	};

};

// collection argument can be collection object or collection name
Mongo.Collection.prototype.join = function(collection, foreignKey, containerField, fieldList) {
	var collectionObject = null;
	var collectionName = "";

	if (_.isString(collection)) {
		collectionName = collection;
	}
	else {
		collectionObject = collection;
	}

	this.doJoin(collectionObject, collectionName, "", foreignKey, containerField, fieldList);
};

Mongo.Collection.prototype.genericJoin = function(collectionNameField, foreignKey, containerField) {
	this.doJoin(null, "", collectionNameField, foreignKey, containerField, []);
};

Mongo.Collection.prototype.publishJoinedCursors = function(cursor) {
	var cursors = [];
	// cursors.push(cursor);




	_.each(this._joins, function(join) {

		if (join.collectionObject || join.collectionName) {
			var coll = null;

			if (join.collectionObject) {
				coll = join.collectionObject;
			}
			else {
				coll = globalContext[join.collectionName];
			}

			if (coll) {
				var collectionName = coll._name;
				var cursorName = cursor._cursorDescription.collectionName;
				//console.log("cursors=",cursors);
				//console.log(" cursor=",cur );
				//console.log( "collection name=",coll._name );
				//console.log( "cursor collectionName=",cursor._cursorDescription.collectionName );

				if (collectionName === cursorName) {
					// it's the same collection, we should merge cursor selector with foreing ids

					console.log("collectionName=", collectionName, " cursorName=", cursorName);

					var fks = cursor.map(function(doc) {
						return doc[join.foreignKey];
					});

					var cursorSelector = cursor._cursorDescription.selector;

					var combinedSelector = {
						$or: [cursorSelector, {
							_id: {
								$in: fks
							}
						}]
					};

					console.log("fks=", fks, " cursorSelector=", cursorSelector, " combinedSelector=", combinedSelector);
					var cur = coll.find(combinedSelector);

					cursors.push(cur);

				}
				else {
					// join from different collections, so we need to create new cursor for the other collection
					cursors.push(cursor);
					var ids = cursor.map(function(doc) {
						return doc[join.foreignKey];
					});
					var cur = coll.find({
						_id: {
							$in: ids
						}
					});
					cursors.push(cur);
				}
			}

		}
		else if (join.collectionNameField) {
			var data = cursor.map(function(doc) {
				var res = {};
				res[join.collectionNameField] = doc[join.collectionNameField];
				res[join.foreignKey] = doc[join.foreignKey];
				return res;
			});

			var collectionNames = _.uniq(_.map(data, function(doc) {
				return doc[join.collectionNameField];
			}));
			_.each(collectionNames, function(collectionName) {
				var coll = globalContext[collectionName];
				if (coll) {
					var ids = _.map(_.filter(data, function(doc) {
						return doc[join.collectionNameField] === collectionName;
					}), function(el) {
						return el[join.foreignKey];
					});
					var cur = coll.find({
						_id: {
							$in: ids
						}
					});
					cursors.push(cur);
				}
			});
		}
	});

	return cursors;
};
