define([
  "handlebars"
], function(Handlebars){

  var errTemplate = Handlebars.compile("<div class='alert alert-danger'>{{message}}</div>");

  return function(message) {
    return {
      render: function() {
        return errTemplate({message: message});
      }
    };
  };
});
