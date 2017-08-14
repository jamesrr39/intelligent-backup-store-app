requirejs.config({
  paths: {
    jquery: "libs/jquery-3.2.1",
    handlebars: "libs/handlebars-4.0.10"
  }
});

define([
  "jquery",
  "./BucketListingView",
  "./BucketView"
], function($, BucketListingView, BucketView){

  var bucketListingView = new BucketListingView();

  var $contentEl = $("#content");

	window.onhashchange = function(){
		var hashFragments = window.location.hash.substring(2).split("/"); // remove the '#/' at the start of the hash

    switch (hashFragments[0]) {
      case "buckets":
        if (hashFragments[1]) {
          var bucketView = new BucketView(hashFragments[1]);
          bucketView.render($contentEl);
          return
        }
        bucketListingView.render($contentEl);
        return;
      default:
        bucketListingView.render($contentEl);
		}
	}

	// render start screen depending on start hash location
	window.onhashchange();
});
