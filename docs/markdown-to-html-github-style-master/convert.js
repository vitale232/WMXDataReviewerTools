var showdown  = require('showdown');
var fs = require('fs');
var resolve = require('path').resolve
let filename = "../../README.md"
let pageTitle = process.argv[2] || ""

fs.readFile(__dirname + '/style.css', function (err, styleData) {
  fs.readFile(process.cwd() + '/' + filename, function (err, data) {
    if (err) {
      throw err; 
    }
    let text = data.toString();

    converter = new showdown.Converter({
      ghCompatibleHeaderId: true,
      simpleLineBreaks: true,
      ghMentions: true,
      tables: true,
      literalMidWordUnderscores: true,
    });

    let preContent = `
    <html>
      <head>
        <title>` + pageTitle + `</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
      </head>
      <body>
        <div id='content'>
    `

    let postContent = `

        </div>
        <style type='text/css'>` + styleData + `</style>
      </body>
    </html>`;

    html = preContent + converter.makeHtml(text) + postContent

    converter.setFlavor('github');

    let filePath = resolve("../../README.html");
    fs.writeFile(filePath, html, { flag: "wx" }, function(err) {
      if (err) {
        console.log("File '" + filePath + "' already exists. Deleting!");
        fs.unlinkSync(filePath);
        fs.writeFile(filePath, html, { flag: "wx" }, function(err) {
          if (err) {
            console.log('Cannot delete file. An error occurred!');
            console.error(err);
          } else {
            console.log('Done, saved to ' + filePath);
          }
        })
      } else {
        console.log("Done, saved to " + filePath);
      }
    });
  });
});
