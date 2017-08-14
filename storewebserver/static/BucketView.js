define([
  "jquery",
  "handlebars"
], function($, Handlebars){

  var template = Handlebars.compile([
    "<div>",
      "<h3>{{bucketName}}</h3>",
      "<p>Showing the latest revision</p>",
      "<div class='file-listing'>",
        "{{#files}}",
          "<div style='float: left; margin: 5px; padding: 3px; border: 1px solid orange;'>{{path}}</div>",
        "{{/files}}",
      "</div>",
    "</div>"
  ].join(""));

  function renderBucketRevisions(bucket, $container) {
    $.ajax("/api/buckets/" + bucket.name).then(function(revisions) {
      $container.html(JSON.stringify(revisions));
    }).fail(function(xhr) {
      $container.html(errTemplate({
        message: "Error fetching revisions data: " + xhr.responseText
      }));
    });
  };

  return function(bucketName) {
    return {
      render($container) {
        $.ajax("/api/buckets/" + encodeURIComponent(bucketName) + "/latest").then(function(data) {
          $container.html(template(data));
        }).fail(function() {
          throw new Error("failed to get bucket latest info");
        });


      }
    };
  };
});
