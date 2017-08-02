define([
  "jquery",
  "handlebars"
], function($, Handlebars){

  var bucketListTemplate = Handlebars.compile([
    "<table class='table'>",
      "<tbody>",
        "{{#buckets}}",
          "<tr>",
            "<td>",
              "{{name}}",
            "</td>",
            "<td class='revisions-listing'></td>",
          "</tr>",
        "{{/buckets}}",
      "</tbody>",
    "</table>"
  ].join(""));

  var errTemplate = Handlebars.compile("<div class='alert alert-danger'>{{message}}</div>");

  return function(){

    function renderBucketListing(buckets, $container) {
      $container.html(bucketListTemplate({buckets: buckets}));
      buckets.forEach((bucket, index) => {
        var $element = $container.find(".revisions-listing").eq(index);
        renderBucketRevisions(bucket, $element);
      });
    }

    function renderBucketRevisions(bucket, $element) {
      $.ajax("/api/buckets/" + bucket.name).then(function(revisions) {
        $element.html(JSON.stringify(revisions));
      }).fail(function(xhr) {
        $element.html(errTemplate({
          message: "Error fetching revisions data: " + xhr.responseText
        }));
      });
    };

    return {
      render: function($container) {
    		$.ajax("/api/buckets/").then(function(buckets){
          renderBucketListing(buckets, $container);
        }).fail(function(xhr) {
    			$container.html(errTemplate({
            message: "Error fetching buckets data: " + xhr.responseText
          }));
    		});
      }
    };
  };
})
