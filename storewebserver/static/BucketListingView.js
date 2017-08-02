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
              "<a href='#/buckets/{{encodedName}}'>",
              "{{name}}",
              "</a>",
            "</td>",
            "<td>",
            "{{lastRevisionDate}}",
            "</td>",
          "</tr>",
        "{{/buckets}}",
      "</tbody>",
    "</table>"
  ].join(""));

  var errTemplate = Handlebars.compile("<div class='alert alert-danger'>{{message}}</div>");

  return function(){

    function renderBucketListing(buckets, $container) {
      $container.html(bucketListTemplate({
        buckets: buckets.map(function(bucket) {
          return {
            encodedName: encodeURIComponent(bucket.name),
            name: bucket.name,
            lastRevisionDate: new Date(bucket.lastRevisionTs * 1000).toLocaleString()
          }
        })
      }));
    }

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
