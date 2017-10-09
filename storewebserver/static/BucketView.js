define([
  "jquery",
  "handlebars",
  "./ErrorView",
  "./FileIcons"
], function($, Handlebars, ErrorView, FileIcons){

  var template = Handlebars.compile([
    "<div>",
      "<h3>{{bucketName}}</h3>",
      "<p>",
        "<select class='form-control' name='change-revision-select'>",
          "{{#revisionTimestamps}}",
            "<option value={{versionTimestamp}} {{selected}}>",
              "{{timestampDisplayString}}",
            "</option>",
          "{{/revisionTimestamps}}",
        "</select>",
      "</p>",
      "<p>",
        "<a href='{{homeURL}}'>",
          "<i class='fa fa-fw fa-home'></i>",
          "Home",
        "</a>",
        "{{#dirLevels}}",
          " / <a href='{{url}}'>{{name}}</a>",
        "{{/dirLevels}}",
      "</p>",
      "<div class='listing-container'>",
        "<div style='clear: both'>",
          "{{#dirs}}",
            "<div class='dir'>",
              "<a href='#/buckets/{{bucketName}}/{{revisionStr}}/{{encodedName}}'>",
                "<div class='name'>",
                  "<i class='icon fa fa-folder' aria-hidden='true'></i>",
                  "{{name}} ({{nestedFileCount}})",
                "</div>",
              "</a>",
            "</div>",
          "{{/dirs}}",
        "</div>",
        "<div style='clear: both'>",
          "{{#files}}",
            "<div class='file'>",
              "<div class='thumbnail'>",
                "<i class='fa fa-{{iconClass}} file-type-icon'></i>",
              "</div>",
              "<div class='name'>",
                "<a href='{{fileURL}}' target='_blank'>",
                "{{name}}",
                "</a>",
              "</div>",
            "</div>",
          "{{/files}}",
        "</div>",
      "</div>",
    "</div>"
  ].join(""));

  function renderBucketRevisions(bucketName, $container) {
    $.when(["/api/buckets/" + encodeURIComponent(bucketName)]).then(function(revisions) {
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

        $.when(
          $.ajax("/api/buckets/" + encodeURIComponent(bucketName)),
          $.ajax(url)
        ).done(function(
          bucketsRequestObject,
          dataRequestObject) {

          var bucket = bucketsRequestObject[0];
          var data = dataRequestObject[0];

          var dirLevels = rootDir.split("/");

          $container.html(template({
            bucketName: data.bucketName,
            revisionTimestamps: bucket.revisions.map(function(revision) {
              return {
                versionTimestamp: revision.versionTimestamp,
                timestampDisplayString: new Date(revision.versionTimestamp * 1000).toString(),
                selected: (revisionStr === (revision.versionTimestamp + "")) ? "selected" : ""
              };
            }).sort(function(a, b) {
              return (a.versionTimestamp < b.versionTimestamp) ? 1 : -1;
            }),
            files: data.files.sort(function(a, b){
              return a.path.toUpperCase() > b.path.toUpperCase();
            }).map(function(file) {
              var lastSlashIndex = file.path.lastIndexOf("/");

              var fileName = (lastSlashIndex === -1) ? file.path : file.path.substring(lastSlashIndex+1);

              return {
                fileURL: "/api/buckets/"+encodeURIComponent(bucketName) +"/"+encodeURIComponent(revisionStr)+"/file?relativePath=" + encodeURIComponent(file.path),
                name: fileName,
                iconClass: FileIcons.classNameFromFileName(fileName)
              }
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
            revisionStr: revisionStr,
            dirLevels: dirLevels.map(function(dirLevelName, index) {
              var relativePath = dirLevels.slice(0, index +1).join("/");

              return {
                url: "#/buckets/" + encodeURIComponent(bucketName) + "/" + revisionStr + "/" + relativePath,
                name: dirLevelName
              };
            }),
            homeURL: "#/buckets/" + encodeURIComponent(bucketName) + "/" + revisionStr
          }));

          $container.find("[name='change-revision-select']").on("change", function(event){
            var newTimestamp = $(event.currentTarget).val();
            window.location = "#/buckets/docs/" + newTimestamp + "/" + rootDir;
          });


        }).fail(function(xhr) {
          $container.html(new ErrorView("Error fetching revisions data: " + xhr.responseText).render());
        });
      }
    };
  };
});
