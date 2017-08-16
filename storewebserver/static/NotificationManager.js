define([
  "jquery",
  "handlebars"
], function($, Handlebars){

  var notificationTemplate = Handlebars.compile([
    "<div style='background-color: red; color: white; margin: 5px'>",
      "<h3>{{title}}</h3>",
      "<p>{{message}}</p>",
    "</div>"
  ].join(""));

  return function(containerEl) {
      return {
        error(title, message, options) {
          options = Object.assign({timeout: 4000}, options);

          var notificationEl = $(notificationTemplate({
            title: title,
            message: message
          }));

          $(containerEl).append(notificationEl);
          window.setTimeout(function(){
            console.log("timeout")
            $(notificationEl).fadeOut(function(){
              $(notificationEl).remove();
            });
          }, options.timeout);
        }
      };
  };
});
