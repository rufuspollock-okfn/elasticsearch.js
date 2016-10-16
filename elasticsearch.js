var ES = {};

(function(my) {
  // use either jQuery or Underscore Deferred depending on what is available
 var Deferred = (typeof jQuery !== "undefined" && jQuery.Deferred) || _.Deferred;

  // ## Table
  //
  // A simple JS wrapper around an [ElasticSearch](http://www.elasticsearch.org/) Type / Table endpoint.
  //
  // @param {String} endpoint: url for ElasticSearch type/table, e.g. for ES running
  // on http://localhost:9200 with index twitter and type tweet it would be:
  // 
  // <pre>http://localhost:9200/twitter/tweet</pre>
  //
  // @param {Object} options: set of options such as:
  //
  // * headers - {dict of headers to add to each request}
  // * dataType: dataType for AJAX requests e.g. set to jsonp to make jsonp requests (default is json requests)
  my.Table = function(endpoint, options) { 
    var self = this;
    this.endpoint = endpoint;
    this.headers = options.headers || {};
    this.options = _.extend({
        dataType: 'json'
      },
      options);

    // ### mapping
    //
    // Get ES mapping for this type/table
    //
    // @return promise compatible deferred object.
    this.mapping = function() {
      var schemaUrl = self.endpoint + '/_mapping';
      var request = _.extend({url: schemaUrl}, this.options);
      var jqxhr = makeRequest(request, this.headers);
      return jqxhr;
    };

    // ### get
    //
    // Get record corresponding to specified id
    //
    // @return promise compatible deferred object.
    this.get = function(id) {
      var base = this.endpoint + '/' + id;
      var request = _.extend({url: base}, this.options);
      return makeRequest(request, this.headers);
    };

    // ### upsert
    //
    // create / update a record to ElasticSearch backend
    //
    // @param {Object} doc an object to insert to the index.
    // @return deferred supporting promise API
    this.upsert = function(doc) {
      var data = JSON.stringify(doc);
      url = this.endpoint;
      if (doc.id) {
        url += '/' + doc.id;
      }
      var request = _.extend({url: base}, {type: 'POST'}, {data: data}, this.options);
      return makeRequest(request, this.headers);
    };

    // ### update
    //
    // update a record to ElasticSearch backend
    //
    // @param {Object} doc an object to update to the index.
    // @param {String} id of the doc to update
    // @return deferred supporting promise API
    this.update = function(doc, doc_id) {
      var upd = { "doc" : doc };
      var data = JSON.stringify({ "doc" : doc })
      var request = _.extend({url: this.endpoint + '/' + doc_id + '/_update'}, {type: 'POST'}, {data: data}, this.options);
      return makeRequest(request, this.headers);
    };

    // ### delete
    //
    // Delete a record from the ElasticSearch backend.
    //
    // @param {Object} id id of object to delete
    // @return deferred supporting promise API
    this.remove = function(id) {
      url = this.endpoint;
      url += '/' + id;
      var request = _.extend({url: url}, {type: 'DELETE'}, this.options);
      return makeRequest(request, this.headers);
    };

    this._normalizeQuery = function(queryObj) {
      var self = this;
      var queryInfo = (queryObj && queryObj.toJSON) ? queryObj.toJSON() : _.extend({}, queryObj);
      var query;
      if (queryInfo.q) {
        query = { 
          query_string : { 
            query : queryInfo.q 
          }  
        }
      } else if (queryInfo.ids) {
        query = {
          ids : {
            values : queryInfo.ids
          }
        }
      } else {
        query = {
          match_all: {}
        }
      }
      var out;
      if (queryInfo.filters && queryInfo.filters.length) {
        // set up filtered query
        out = { 
          filtered : { 
            filter : { 
              and : []
            }
          }
        };
        // add filters
        _.each(queryInfo.filters, function(filter) {
          out.filtered.filter.and.push(self._convertFilter(filter));
        });
	// add query string only if needed
	if (queryInfo.q || queryInfo.ids) {
	  out.filtered.query = query;
	}
      } else {
        out = {
          constant_score: { query: {} }
        };
        out.constant_score.query = query;
      }
      return out;
    },

    // convert from Recline sort structure to ES form
    // http://www.elasticsearch.org/guide/reference/api/search/sort.html
    this._normalizeSort = function(sort) {
      var out = _.map(sort, function(sortObj) {
        var _tmp = {};
        var _tmp2 = _.clone(sortObj);
        delete _tmp2['field'];
        _tmp[sortObj.field] = _tmp2;
        return _tmp;
      });
      return out;
    },

    this._convertFilter = function(filter) {
      var out = {};
      out[filter.type] = {};
      if (filter.type === 'term') {
        out.term[filter.field] = filter.term;
      } else if (filter.type === 'terms') {
        // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-terms-filter.html
        out.terms[filter.field] = filter.terms;
        if ('execution' in filter) {
            out.terms['execution'] = filter.execution;
        }
      } else if (filter.type === 'geo_distance') {
        out.geo_distance[filter.field] = filter.point;
        out.geo_distance.distance = filter.distance;
        out.geo_distance.unit = filter.unit;
      } else if (filter.type === 'range') {
        // range filter: http://www.elasticsearch.org/guide/reference/query-dsl/range-filter/
        out.range[filter.field] = { 
          from : filter.from, 
          to : filter.to 
        };
        if (_.has(filter, 'include_lower')) {
          out.range[filter.field].include_lower = filter.include_lower;
        }
        if (_.has(filter, 'include_upper')) {
          out.range[filter.field].include_upper = filter.include_upper;
        }
      } else if (filter.type == 'type') {
        // type filter: http://www.elasticsearch.org/guide/reference/query-dsl/type-filter/
        out.type = { value : filter.value };
      } else if (filter.type == 'exists') {
        // exists filter: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-exists-filter.html
        out.exists = { field : filter.field };
      } else if (filter.type == 'missing') {
        // missing filter: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-missing-filter.html
        out.missing = { field : filter.field };
      }
      if (filter.not) {
        out = { not: JSON.parse(JSON.stringify(out)) };
      }
      return out;
    },

    // ### query
    //
    // @return deferred supporting promise API
    this.query = function(queryObj) {
      var esQuery = (queryObj && queryObj.toJSON) ? queryObj.toJSON() : _.extend({}, queryObj);
      esQuery.query = this._normalizeQuery(queryObj);
      delete esQuery.q;
      delete esQuery.filters;
      if (esQuery.sort && esQuery.sort.length > 0) {
        esQuery.sort = this._normalizeSort(esQuery.sort);
      }
      if (esQuery.ids) {
        esQuery.size = esQuery.ids.length;
        delete esQuery.ids;
      }        
      var data = {source: JSON.stringify(esQuery)};
      var url = this.endpoint + '/_search';
      var request = _.extend({url: url}, {type: 'POST'}, {data: JSON.stringify(esQuery)}, this.options);
      var jqxhr = makeRequest(request, this.headers);
      return jqxhr;
    };
  };

// ### makeRequest
// 
// Just $.ajax but in any headers in the 'headers' attribute of this
// Backend instance. Example:
//
// <pre>
// var jqxhr = this._makeRequest({
//   url: the-url
// });
// </pre>
var makeRequest = function(data, headers) {
  var extras = {};
  if (headers) {
    extras = {
      beforeSend: function(req) {
        _.each(headers, function(value, key) {
          req.setRequestHeader(key, value);
        });
      }
    };
  }
  var data = _.extend(extras, data);
  return jQuery.ajax(data);
};

}(ES));

var recline = recline || {};
recline.Backend = recline.Backend || {};
recline.Backend.ElasticSearch = recline.Backend.ElasticSearch || {};

(function(my) {
  "use strict";
  my.__type__ = 'elasticsearch';

  // use either jQuery or Underscore Deferred depending on what is available
  var Deferred = (typeof jQuery !== "undefined" && jQuery.Deferred) || _.Deferred;

  // ## Recline Connectors 
  //
  // Requires URL of ElasticSearch endpoint to be specified on the dataset
  // via the url attribute.

  // ES options which are passed through to `options` on Wrapper (see Wrapper for details)
  my.esOptions = {};

  // ### fetch
  my.fetch = function(dataset) {
    var es = new ES.Table(dataset.url, my.esOptions);
    var dfd = new Deferred();
    es.mapping().done(function(schema) {

      if (!schema){
        dfd.reject({'message':'Elastic Search did not return a mapping'});
        return;
      }

      // only one top level key in ES = the type so we can ignore it
      var key = _.keys(schema)[0];
      var fieldData = _.map(schema[key].properties, function(dict, fieldName) {
        dict.id = fieldName;
        return dict;
      });
      dfd.resolve({
        fields: fieldData
      });
    })
    .fail(function(args) {
      dfd.reject(args);
    });
    return dfd.promise();
  };

  // ### save
  my.save = function(changes, dataset) {
    var es = new ES.Table(dataset.url, my.esOptions);
    if (changes.creates.length + changes.updates.length + changes.deletes.length > 1) {
      var dfd = new Deferred();
      msg = 'Saving more than one item at a time not yet supported';
      alert(msg);
      dfd.reject(msg);
      return dfd.promise();
    }
    if (changes.creates.length > 0) {
      return es.upsert(changes.creates[0]);
    }
    else if (changes.updates.length >0) {
      return es.upsert(changes.updates[0]);
    } else if (changes.deletes.length > 0) {
      return es.remove(changes.deletes[0].id);
    }
  };

  // ### update
  my.update = function(doc, doc_id, dataset) {
    var es = new ES.Table(dataset.url, my.esOptions);
    return es.update(doc, doc_id);
  };

  // ### query
  my.query = function(queryObj, dataset) {
    var dfd = new Deferred();
    var es = new ES.Table(dataset.url, my.esOptions);
    var jqxhr = es.query(queryObj);
    jqxhr.done(function(results) {
      var out = {
        total: results.hits.total
      };
      out.hits = _.map(results.hits.hits, function(hit) {
        if (!('id' in hit._source) && hit._id) {
          hit._source.id = hit._id;
        }
        return hit._source;
      });
      if (results.facets) {
        out.facets = results.facets;
      }
      dfd.resolve(out);
    }).fail(function(errorObj) {
      var out = {
        title: 'Failed: ' + errorObj.status + ' code',
        message: errorObj.responseText
      };
      dfd.reject(out);
    });
    return dfd.promise();
  };
}(recline.Backend.ElasticSearch));

