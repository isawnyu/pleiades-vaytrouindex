"""Pluggable Vaytrou-based spatial indexes"""

import logging
import os
from urllib import urlencode

import Globals  # import Zope 2 dependencies in order
from BTrees.IIBTree import IIBTree, IITreeSet, IISet, union, intersection
from httplib2 import Http
from OFS.PropertyManager import PropertyManager
from OFS.SimpleItem import SimpleItem
from Products.CMFCore.utils import _getAuthenticatedUser, getToolByName
from Products.PluginIndexes.common.util import parseIndexRequest
from Products.PluginIndexes.interfaces import IPluggableIndex
from simplejson import dumps, loads
from transaction.interfaces import IDataManager
import transaction
from pleiades.geographer.geo import NotLocatedError
from zgeo.geographer.interfaces import IGeoreferenced
from zope.interface import implements

from pleiades.vaytrouindex.interfaces import IVaytrouConnectionManager
from pleiades.vaytrouindex.interfaces import IVaytrouIndex

log = logging.getLogger('pleiades.vaytrou')


from plone.indexer.decorator import indexer
from Products.PleiadesEntity.content.interfaces import ILocation
@indexer(ILocation)
def location_geolocation(object, **kw):
     return IGeoreferenced(object)


class VaytrouIndex(PropertyManager, SimpleItem):
    # Inspired by and derived from alm.solrindex's SolrIndex

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
        )

    manage_options = PropertyManager.manage_options + SimpleItem.manage_options

    _v_temp_cm = None
    vaytrou_uri_static = ''
    vaytrou_uri_env_var = ''
    query_options = ['query', 'range']

    def __init__(self, id, vaytrou_uri_static=''):
        self.id = id
        self.vaytrou_uri_static = vaytrou_uri_static

    @property
    def vaytrou_uri(self):
        if self.vaytrou_uri_static:
            return self.vaytrou_uri_static
        elif self.vaytrou_uri_env_var:
            return os.environ[self.vaytrou_uri_env_var]
        else:
            raise ValueError("No Vaytrou URI provided")

    @property
    def connection_manager(self):
        jar = self._p_jar
        oid = self._p_oid

        if jar is None or oid is None:
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
        return [self.getId()]

    def getEntryForObject(self, documentId, default=None):
        """Return the information stored for documentId"""
        cm = self.connection_manager
        try:
            response = cm.connection.items(documentId)
            return [dict(
                geometry=item.get('geometry'), bbox=item.get('bbox')
                ) for item in response['items']]
        except (VaytrouConnectionError, VaytrouHTTPError):
            return None

    def index_object(self, documentId, obj, threshold=None):
        """Index an object.

        'documentId' is the integer ID of the document.
        'obj' is the object to be indexed.
        """
        log.info("Indexing %s: %s, %s", self.getId(), documentId, obj)
        cm = self.connection_manager
        portal_path = obj.portal_url.getPortalObject().getPhysicalPath()
        def wrap(ob):
            try:
                ob_path = ob.getPhysicalPath()[len(portal_path):]
                g = IGeoreferenced(ob)
                return dict(
                    id=ob.getPhysicalPath(),
                    bbox=g.bounds,
                    properties=dict(
                        path='/'.join(ob_path),
                        pid=ob.getId(),
                        title=ob.Title(),
                        description=ob.Description(),
                        ),
                    geometry=dict(type=g.type, coordinates=g.coordinates)
                    )
            except (AttributeError, NotLocatedError, TypeError, ValueError), e:
                log.warn("Failed to wrap ob %s: %s", ob, str(e))
                raise
                return 0
        o = wrap(obj)
        if o is None:
            return 0
        try:
            o['id'] = str(documentId)
            doc = dict(index=[o])
            cm.connection.batch(doc)
            log.debug("Passed index_doc %s", documentId)
        except Exception, e:
            log.warn("Failed to index_doc %s: %s", documentId, str(e))
        return 1

    def unindex_object(self, documentId):
        """Remove the documentId from the index."""
        log.debug("Unindexing %d", documentId)
        cm = self.connection_manager
        try:
            item = cm.connection.items(documentId)['items'][0]
            doc = dict(unindex=[item])
            cm.connection.batch(doc)
            log.debug("Passed unindex_doc %s", documentId)
        except Exception, e:
            log.warn("Failed to unindex_doc %s: %s", documentId, str(e))
            return 0
        return 1

    def _apply_index(self, request, cid='', raw=False):
        """Apply query specified by request, a mapping containing the query.

        Returns two objects on success: the resultSet containing the
        matching record numbers, and a tuple containing the names of
        the fields used.

        Returns None if request is not valid for this index.

        If ``raw``, returns the raw response from the index server as a 
        mapping.
        """
        record = parseIndexRequest(request, self.getId(), self.query_options)
        if record.keys == None:
            return None
        params = {'query': record.keys, 'range': record.range}

        log.debug("querying: %r", params)

        cm = self.connection_manager
        try:
            response = cm.connection.query(
                params['range'], params['query'])
            if raw:
                return response
            result = IIBTree()
            for item in response:
                score = int(float(item.get('score', 0)) * 1000)
                result[int(item['id'])] = score
            return result, (self.getId(),)
        except Exception, e:
            log.warn("Failed to apply %s: %s", params, str(e))
            return None

    def numObjects(self):
        """Return number of unique words in the index"""
        return 0

    def indexSize(self):
        """Return the number of indexed objects"""
        cm = self.connection_manager
        try:
            response = cm.connection.info()
            return int(response['num_items'])
        except VaytrouHTTPError:
            return 0

    def clear(self):
        """Empty the index"""
        cm = self.connection_manager
        try:
            response = cm.connection.clear()
            return response
        except VaytrouHTTPError:
            return 0


class LocationContainerIndex(PropertyManager, SimpleItem):
    """Finds containers of spatially indexed objects
    
    A facade, does not index or unindex docs, only looks up objects
    in a specified VaytrouIndex and returns index keys of their containers.
    """

    implements(IPluggableIndex)
    geoindex_id = ''
    query_options = ['query', 'range']
    _properties = (
        {'id': 'geoindex_id', 
         'type': 'string', 
         'mode': 'w',
         'description':
           'The identifier of the Vaytrou Index, for example, "geolocation"'},
        )

    manage_options = PropertyManager.manage_options + SimpleItem.manage_options

    def __init__(self, id, geoindex_id=''):
        self.id = id
        self.geoindex_id = geoindex_id

    def getIndexSourceNames(self):
        return [self.getId()]

    def getEntryForObject(self, documentId, default=None):
        # TODO
        return None

    def index_object(self, documentId, obj, threshold=None):
        return 1

    def unindex_object(self, documentId):
        return 1

    def _apply_index(self, request, cid=''):
        record = parseIndexRequest(request, self.getId(), self.query_options)
        if record.keys == None:
            return None
        
        catalog = getToolByName(self, 'portal_catalog')

        geoIndex = catalog._catalog.getIndex(self.geoindex_id)
        geoRequest = {}
        geoRequest[self.geoindex_id] = {
            'query': record.keys, 'range': record.range}
        geo_response = geoIndex._apply_index(geoRequest, raw=True)

        paths = {}
        for item in geo_response:
            paths[int(item['id'])] = item['properties']['path']

        #ptIndex = catalog._catalog.getIndex('portal_type')
        #pt_set = ptIndex._apply_index({'portal_type': ['Location']})[0]

        rolesIndex = catalog._catalog.getIndex('allowedRolesAndUsers')
        user = _getAuthenticatedUser(self)
        perms_set = rolesIndex._apply_index(
            {'allowedRolesAndUsers': catalog._listAllowedRolesAndUsers(user)}
            )[0]

        #r = intersection(pt_set, perms_set)
        r = intersection(perms_set, IISet(paths.keys()))

        if isinstance(r, int):  r=IISet((r,))
        if r is None:
            return IISet(), (self.getId(),)
        else:
            def up(path):
                return '/plone' + '/'.join(path.rstrip('/').split('/')[:-1])
            return IISet(
                [catalog.getrid(up(paths[lid])) for lid in r]), (self.getId(),)

    def numObjects(self):
        return 0

    def indexSize(self):
        return 0

    def clear(self):
        pass


# Vaytrou index HTTP client

class NoRollbackSavepoint:
    def __init__(self, datamanager):
        self.datamanager = datamanager
    def rollback(self):
        pass

class Error(Exception):
    pass

class VaytrouConnectionError(Error):
    def __init__(self, resp):
        self.resp = resp
    def __str__(self):
        return str(self.resp)

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
        try:
            resp, content = h.request(self.uri, "GET")
        except Exception, e:
            raise VaytrouConnectionError(e)
        if resp.status != 200:
            raise VaytrouHTTPError(resp)
        return loads(content)
        
    def items(self, docId):
        h = Http(timeout=1000)
        try:
            resp, content = h.request(
                self.uri + '/items/%s' % str(docId), "GET")
        except Exception, e:
            raise VaytrouConnectionError(e)
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
        try:
            resp, content = h.request(self.uri, "POST", body=dumps(doc))
        except Exception, e:
            raise VaytrouConnectionError(e)
        if resp.status != 200:
            raise VaytrouHTTPError(resp)
        return 1
    def clear(self):
        h = Http(timeout=1000)
        doc = {'clear': True}
        try:
            resp, content = h.request(self.uri, "POST", body=dumps(doc))
            log.debug("Index cleared.")
        except Exception, e:
            raise VaytrouConnectionError(e)
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

