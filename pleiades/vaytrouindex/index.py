"""VaytrouIndex and HTTPConnectionManager"""

import logging
import os
from urllib import urlencode

from httplib2 import Http
from simplejson import dumps, loads

import Globals  # import Zope 2 dependencies in order
from BTrees.IIBTree import IIBTree, IISet
from OFS.PropertyManager import PropertyManager
from OFS.SimpleItem import SimpleItem
from transaction.interfaces import IDataManager
import transaction
from zope.interface import implements

from zgeo.geographer.interfaces import IGeoreferenced
from pleiades.geographer.geo import NotLocatedError
from pleiades.vaytrouindex.interfaces import IVaytrouConnectionManager
from pleiades.vaytrouindex.interfaces import IVaytrouIndex

log = logging.getLogger(__name__)


class VaytrouIndex(PropertyManager, SimpleItem):

    implements(IVaytrouIndex)

    _properties = (
        {'id': 'vaytrou_uri_static', 'type': 'string', 'mode': 'w',
            'description':
            'The Vaytrou URI, for example, "http://localhost:8888/". '
            'You should leave this empty if you set vaytrou_uri_env_var.'},
        {'id': 'vaytrou_uri_env_var', 'type': 'string', 'mode': 'w',
            'description':
            'The name of an environment variable that will provide '
            'the Vaytrou URI.  Ignored if vaytrou_uri_static is non-empty.'},
        {'id': 'vaytrou_uri', 'type': 'string', 'mode': '',
            'description': 'The effective Vaytrou URI (read-only)'},
        )

    manage_options = PropertyManager.manage_options + SimpleItem.manage_options

    _v_temp_cm = None  # An IHTTPConnectionManager used during initialization
    vaytrou_uri_static = ''
    vaytrou_uri_env_var = ''

    def __init__(self, id, vaytrou_uri_static=''):
        self.id = id
        self.vaytrou_uri_static = vaytrou_uri_static

    @property
    def vaytrou_uri(self):
        if self.vaytrou_uri_static:
            return self.vaytrou_uri_static
        elif self.vaytrou_uri_env_var:
            return os.environ[self.vaytrou_uri_env_var]
        elif 'vaytrou_uri' in self.__dict__:
            # b/w compat
            return self.__dict__['vaytrou_uri']
        else:
            raise ValueError("No Vaytrou URI provided")

    @property
    def connection_manager(self):
        jar = self._p_jar
        oid = self._p_oid

        if jar is None or oid is None:
            # Not yet stored in ZODB, so use _v_temp_cm
            manager = self._v_temp_cm
            if manager is None or manager.vaytrou_uri != self.vaytrou_uri:
                self._v_temp_cm = manager = IVaytrouConnectionManager(self)

        else:
            fc = getattr(jar, 'foreign_connections', None)
            if fc is None:
                jar.foreign_connections = fc = {}

            manager = fc.get(oid)
            if manager is None or manager.vaytrou_uri != self.vaytrou_uri:
                manager = IVaytrouConnectionManager(self)
                fc[oid] = manager

        return manager

    def getIndexSourceNames(self):
        """Get a sequence of attribute names that are indexed by the index.
        """
        return ['geolocation']

    def getEntryForObject(self, documentId, default=None):
        """Return the information stored for documentId"""
        cm = self.connection_manager
        try:
            response = cm.connection.items(documentId)
            return list(response)[0]
        except VaytrouHTTPError:
            return None

    def index_object(self, documentId, obj, threshold=None):
        """Index an object.

        'documentId' is the integer ID of the document.
        'obj' is the object to be indexed.
        'threshold' is the number of words to process between committing
        subtransactions.  If None, subtransactions are disabled.
        """
        cm = self.connection_manager
        portal_path = obj.portal_url.getPortalObject().getPhysicalPath()
        def wrap(ob):
            try:
                ob_path = ob.getPhysicalPath()[len(portal_path):]
                g = IGeoreferenced(ob)
                return dict(
                    id=ob.UID(),
                    bbox=g.bounds,
                    properties=dict(
                        path='/'.join(ob_path),
                        pid=ob.getId(),
                        title=ob.Title(),
                        description=ob.Description() or ob.getDescription(),
                        ),
                    geometry=dict(type=g.type, coordinates=g.coordinates)
                    )
            except (AttributeError, NotLocatedError, TypeError):
                return None
        o = wrap(obj)
        if o is None:
            return 0
        o['id'] = str(documentId)
        doc = dict(index=[o])
        log.debug("indexing %d", documentId)
        cm.connection.batch(doc)
        return 1

    def unindex_object(self, documentId):
        """Remove the documentId from the index."""
        cm = self.connection_manager
        try:
            item = cm.connection.items(documentId)[0]
            doc = dict(unindex=[item])
            log.debug("unindexing %d", documentId)
            cm.connection.batch(doc)
        except Exception, e:
            log.warn("Failed to unindex_doc %s: %s", documentId, str(e))
        return 1

    def _apply_index(self, request, cid=''):
        """Apply query specified by request, a mapping containing the query.

        Returns two objects on success: the resultSet containing the
        matching record numbers, and a tuple containing the names of
        the fields used.

        Returns None if request is not valid for this index.
        """
        cm = self.connection_manager
        q = []           # List of query texts to pass as "q"
        queried = []     # List of field names queried
        vaytrou_params = {}

        # Get the Vaytrou parameters from the catalog query
        #if request.has_key('vaytrou_params'):
        #    vaytrou_params.update(request['vaytrou_params'])

        if request.has_key('geolocation'):
            vaytrou_params.update(request['geolocation'])

        if not vaytrou_params:
            return None

        log.debug("querying: %r", vaytrou_params)

        try:
            response = cm.connection.query(
                vaytrou_params['range'], vaytrou_params['query'])
            result = IIBTree()
            for item in response:                
                # TODO: provide scores from vaytrou server
                score = int(float(item.get('score', 0)) * 1000)
                result[int(item['id'])] = score
            return result, ('geolocation',)
        except Exception, e:
            log.warn("Failed to apply %s: %s", vaytrou_params, str(e))
            return None

    ## The ZCatalog Index management screen uses these methods ##

    def numObjects(self):
        """Return number of unique words in the index"""
        return 0

    def indexSize(self):
        """Return the number of indexed objects"""
        cm = self.connection_manager
        response = cm.connection.info()
        return int(response['num_items'])

    def clear(self):
        """Empty the index"""
        cm = self.connection_manager
        cm.set_changed()
        cm.connection.delete_query()


class NoRollbackSavepoint:

    def __init__(self, datamanager):
        self.datamanager = datamanager

    def rollback(self):
        pass


class Error(Exception):
    pass

class VaytrouHTTPError(Error):
    def __init__(self, resp):
        self.resp = resp
    def __str__(self):
        return str(self.resp)


class VaytrouConnection(object):
    
    def __init__(self, uri):
        self.uri = uri

    def info(self):
        h = Http(timeout=1000)
        resp, content = h.request(self.uri, "GET")
        if resp.status != 200:
            raise VaytrouHTTPError(resp)
        return loads(content)
        
    def items(self, docId):
        h = Http(timeout=1000)
        try:
            resp, content = h.request(
                self.uri + '/items/%s' % str(docId), "GET")
        except Exception, e:
            raise VaytrouHTTPError(e)
        if resp.status != 200:
            raise VaytrouHTTPError(resp)
        return loads(content)
        
    def query(self, range, geom):
        # Only supporting intersection at the moment
        if range == 'intersection':
            bbox = ','.join(map(str, geom))
            data = dict(bbox=bbox, start=0)
        elif range == 'distance':
            data = dict(lon=geom[0][0], lat=geom[0][1], radius=geom[1], start=0)
        elif range == 'nearest':
            bbox = ','.join(map(str, geom[0]))
            data = dict(bbox=bbox, limit=geom[1], start=0)
        h = Http(timeout=1000)
        results = []
        N = 1
        try:
            while len(results) < N:
                resp, content = h.request(
                    self.uri + '/%s?%s' % (
                                range, urlencode(data)),
                    "GET")
                r = loads(content)
                N = r['hits']
                results += r['items']
                data['start'] += r['count']
        except:
            raise
        return results

    def batch(self, doc):
        h = Http(timeout=1000)
        resp, content = h.request(self.uri, "POST", body=dumps(doc))
        if resp.status != 200:
            raise VaytrouHTTPError(resp)
        return 1

    def commit(self):
        pass

    def delete_query(self):
        pass

class VaytrouConnectionManager(object):
    implements(IVaytrouConnectionManager, IDataManager)

    def __init__(self, vaytrou_index, connection_factory=VaytrouConnection):
        self.vaytrou_uri = vaytrou_index.vaytrou_uri
        self._joined = False
        self._connection_factory = connection_factory
        self._connection = connection_factory(self.vaytrou_uri)

    @property
    def connection(self):
        c = self._connection
        if c is None:
            c = self._connection_factory(self.vaytrou_uri)
            self._connection = c
        return c

    def set_changed(self):
        if not self._joined:
            transaction.get().join(self)
            self._joined = True

    def abort(self, transaction):
        try:
            c = self._connection
            if c is not None:
                self._connection = None
                c.close()
        finally:
            self._joined = False

    def tpc_begin(self, transaction):
        pass

    def commit(self, transaction):
        pass

    def tpc_vote(self, transaction):
        # ensure connection is open
        dummy = self.connection

    def tpc_finish(self, transaction):
        try:
            try:
                self.connection.commit()
            except:
                self.abort(transaction)
                raise
        finally:
            self._joined = False

    def tpc_abort(self, transaction):
        pass

    def sortKey(self):
        return self.vaytrou_uri

    def savepoint(self, optimistic=False):
        return NoRollbackSavepoint(self)
