define([
  "jquery",
  "handlebars",
  "./ErrorView"
], function($, Handlebars, ErrorView){

  var template = Handlebars.compile([
    "<div>",
      "<h3>{{bucketName}}</h3>",
      "<p>Showing the latest revision</p>",
      "<div class='dir-listing'>",
        "{{#dirs}}",
          "<div class='dir'>",
            "<a href='#/buckets/{{bucketName}}/{{revisionStr}}/{{encodedName}}'>",
              "{{name}} ({{nestedFileCount}})",
            "</a>",
          "</div>",
        "{{/dirs}}",
      "</div>",
      "<div class='file-listing'>",
        "{{#files}}",
          "<div class='file'>{{path}}</div>",
        "{{/files}}",
      "</div>",
    "</div>"
  ].join(""));

  function renderBucketRevisions(bucket, $container) {
    $.ajax("/api/buckets/" + bucket.name).then(function(revisions) {
      $container.html(JSON.stringify(revisions));
    }).fail(function(xhr) {
      $container.html(new ErrorView("Error fetching revisions data: " + xhr.responseText).render());
    });
  };

  return function(bucketName, revisionStr, rootDir) {

    return {
      render($container) {
        var url = "/api/buckets/" + encodeURIComponent(bucketName) + "/" + revisionStr;
        if (rootDir) {
          url += "?rootDir=" + encodeURIComponent(rootDir);
        }

        $.ajax(url).then(function(data) {
          $container.html(template({
            bucketName: data.bucketName,
            files: data.files.sort(function(a, b){
              return a.path.toUpperCase() > b.path.toUpperCase();
            }),
            dirs: data.dirs.sort(function(a, b){
              return a.name.toUpperCase() > b.name.toUpperCase();
            }).map(function(dir) {
              var namePrefix = rootDir ? rootDir + "/" : "";

              return Object.assign({
                encodedName: namePrefix + encodeURIComponent(dir.name),
                bucketName: bucketName,
                revisionStr: revisionStr
              }, dir);
            }),
            revisionStr: revisionStr
          }));
        }).fail(function(xhr) {
          $container.html(new ErrorView("Error fetching revisions data: " + xhr.responseText).render());
        });
      }
    };
  };
});
