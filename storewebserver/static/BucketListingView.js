define([
  "jquery",
  "handlebars",
  "./ErrorView"
], function($, Handlebars, ErrorView){

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
    			$container.html(new ErrorView("Error fetching buckets data: " + xhr.responseText).render());
    		});
      }
    };
  };
})
