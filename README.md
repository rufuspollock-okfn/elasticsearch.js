A simple javascript library for working with [ElasticSearch][].

It also provides a backend interface to ElasticSearch suitable for use with the
[Recline][] suite of data libraries.

[ElasticSearch]: http://www.elasticsearch.org/
[Recline]: http://okfnlabs.org/recline/

## Usage

In your web page insert:

    <script type="text/javascript" src=""><script>

You should replace the src url with the url to your copy of elasticsearch.js.

Here's an example of using the library to create, get and query some data. Note this example assumes you have ElasticSearch running at http://localhost:9200/

    // Table = ES Type (aka Table)
    // http://www.elasticsearch.org/guide/reference/glossary/#type
    var endpoint = 'http://localhost:9200/twitter/tweet';
    var table = ES.Table(endpoint);
    // get the mapping for this "table"
    // http://www.elasticsearch.org/guide/reference/glossary/#mapping
    table.mapping().done(function(theMapping) {
      console.log(theMapping)
    });

    table.upsert({
      id: '123',
      title: 'My new tweet'
    }).done(function() {
      table.get('123').done(function(doc) {
        console.log(doc);
      });
    });

## Dependencies

* underscore
* jQuery (optional) - only if you want ajax requests
* underscore.deferred (optional) - only needed if no jQuery

One of the reasons for the different options is that it ensures you can use
this library in the browser *and* in webworkers (where jQuery does not
function).

