define(function() {
  return {
    /**
     * with prefex 'fa fa-' (example: fa fa-file-pdf-o)
     * @param {string} typeName
     */
    classNameFromFileName: function(fileName){
      if (fileName.endsWith(".tar.gz")) {
        return "file-archive-o";
      }

      var extension = fileName;
      var lastIndexOfDot = fileName.lastIndexOf(".");
      if (lastIndexOfDot > -1) {
        extension = fileName.substring(lastIndexOfDot);
      }

      switch (extension) {
        case ".pdf":
          return "file-pdf-o";
        case ".zip":
        case ".tar":
          return "file-archive-o";
        case ".xls":
        case ".xlsx":
          return "file-excel-o";
        case ".doc":
        case ".docx":
        case ".odf":
          return "file-word-o";
        case ".ppt":
        case ".pptx":
          return "file-powerpoint-o";
        case ".jpg":
        case ".jpeg":
        case ".bmp":
        case ".png":
        case ".gif":
          return "file-picture-o";
        case ".js": // TODO listing of all code formats?
          return "file-code-o";
        default:
          return "file-o";
      }
    }
  };
});
