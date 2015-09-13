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
					if (Array.isArray(fk) == false) {
						fk = [fk];
					}

					var data = __original.find.call(coll, {
						_id: {
							$in: fk
						}
					}, opt).fetch();
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
						console.log("isArray is true fk=", fkId);
						var data = __original.find.call(coll, {
							_id: {
								$in: fkId
							}
						}, opt).fetch();
						// console.log("data=",data);
						var container = join.containerField || coll._name + "_joined";
						doc[container] = data;
						// console.log("doc=",doc);
					}
					else {

						console.log("pks=", fkId);
						var data = __original.findOne.call(coll, {
							_id: fkId
						}, opt);
						// console.log("data=", data);
						var container = join.containerField || coll._name + "_joined";
						doc[container] = data;
						// console.log("doc=",doc);


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
	// console.log("publishJoinedCursors");
	var mapOfCursors = {};
	var cursorCollectionName = cursor._cursorDescription.collectionName;
	_.each(this._joins, function(join) {
		debugger;
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
				var oldCursor = mapOfCursors[collectionName];
				if (oldCursor == null) {
					mapOfCursors[collectionName] = cursor;
					var ids = cursor.map(function(doc) {
						var a = doc[join.foreignKey];
						return a;
					});
					if (Array.isArray(ids)) {
						ids = _.flatten(ids);
					}
					else {
						ids = [ids];
					}
					var cur = coll.find({
						_id: {
							$in: ids
						}
					});
					mapOfCursors[coll._name] = cur;
				}
				else {
					// do the joining, now oldCursor is pg and cursor is pg also
					var fks = cursor.map(function(doc) {
						var a = doc[join.foreignKey];
						return a;
					});

					if (Array.isArray(fks)) {
						fks = _.flatten(fks);
					}
					else {
						fks = [fks];
					}

					var cursorSelector = oldCursor._cursorDescription.selector;

					var combinedSelector = {
						$or: [cursorSelector, {
							_id: {
								$in: fks
							}
						}]
					};

					console.log("fks=", fks, " cursorSelector=", cursorSelector, " combinedSelector=", combinedSelector);
					var cur = coll.find(combinedSelector);
					mapOfCursors[coll._name] = cur;
				}
			}
		}

		else if (join.collectionNameField) {
			console.log("join.collectionNameField=", join.collectionNameField);

		}
	});

	if (mapOfCursors[cursorCollectionName] != null) {
		// console.log("collection already found in map");
		var oldCursor = mapOfCursors[cursorCollectionName];

		var cursorSelector = cursor._cursorDescription.selector;
		var oldCursorSelector = oldCursor._cursorDescription.selector;
		
		var combinedSelector = {
			$or: [cursorSelector, oldCursorSelector]
		};

		oldCursor._cursorDescription.selector = combinedSelector;
		
		mapOfCursors[cursorCollectionName] = oldCursor;
		
	}
	else {
		// console.log("collection NOT found in map");
		mapOfCursors[cursorCollectionName] = cursor;
	}

	var cursors = _.values(mapOfCursors);
	// console.log("cursors=", cursors);

	return cursors;
};