Package.describe({
	summary: "Generic joins for Meteor",
	version: "1.0.7",
	git: "https://github.com/perak/meteor-joins.git",
	name: "perak:joins"
});

Package.onUse(function (api) {
//	api.use(["mongo", "underscore"]);

	if(api.versionsFrom) {
		api.versionsFrom('METEOR@0.9.0');
	}


	api.add_files('lib/joins.js', ["client", "server"]);
});
