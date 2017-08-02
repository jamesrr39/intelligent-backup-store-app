requirejs.config({
  paths: {
    jquery: "libs/jquery-3.2.1",
    handlebars: "libs/handlebars-4.0.10"
  }
});

define([
  "jquery",
  "./BucketListingView"
], function($, BucketListingView){

  var bucketListingView = new BucketListingView();
  var $contentEl = $("#content");

	window.onhashchange = function(){
		var hash = window.location.hash;

		if (hash.startsWith("#/buckets")) {
			bucketListingView.render($contentEl);
      return;
		}

    // default
    bucketListingView.render($contentEl);
	}

	// render start screen depending on start hash location
	window.onhashchange();
});
