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

  var currentView;

	window.onhashchange = function(){
		var hashFragments = window.location.hash.substring(2).split("/").map(function(fragment){
      return decodeURIComponent(fragment);
    }); // remove the '#/' at the start of the hash
    var bucketName;
    var revisionStr;
    var rootDir;

    if (currentView && currentView.onClose) {
      currentView.onClose();
    }

    switch (hashFragments[0]) {
      case "buckets":
        bucketName = hashFragments[1];
        revisionStr = hashFragments[2];
        if (!revisionStr) {
          window.location.hash = window.location.hash + "/latest";
          return;
        }
        rootDir = hashFragments.slice(3).join("/");

        if (bucketName) {
          var bucketView = new BucketView(bucketName, revisionStr, rootDir);
          currentView = bucketView;
          bucketView.render($contentEl);
          return
        }
        currentView = bucketListingView;
        bucketListingView.render($contentEl);
        return;
      default:
        currentView = bucketListingView;
        bucketListingView.render($contentEl);
		}
	}

	// render start screen depending on start hash location
	window.onhashchange();
});
