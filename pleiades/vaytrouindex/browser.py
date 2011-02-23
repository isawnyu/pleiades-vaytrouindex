
from pleiades.vaytrouindex.index import VaytrouIndex, LocationQueryIndex


class VaytrouIndexAddView:

    def __call__(self, id='', vaytrou_uri='', submit_add='',
            delete_redundant=False):

        if submit_add and id and vaytrou_uri:
            obj = VaytrouIndex(id, vaytrou_uri)
            zcatalog = self.context.context
            catalog = zcatalog._catalog
            catalog.addIndex(id, obj)

            zcatalog._p_jar.add(obj)
            cm = obj.connection_manager
            if delete_redundant:
                for field in cm.schema.fields:
                    name = field.name
                    if name != id and catalog.indexes.has_key(name):
                        catalog.delIndex(name)

            self.request.response.redirect(
                zcatalog.absolute_url() +
                "/manage_catalogIndexes?manage_tabs_message=Index%20Added")

        # Note the unfortunate homonym "index": self.index() renders the add
        # form, which submits to this method to add a catalog index.
        return self.index()


class LocationQueryIndexAddView:

    def __call__(self, id='', submit_add='',
            delete_redundant=False):

        if submit_add and id:
            obj = LocationQueryIndex(id)
            zcatalog = self.context.context
            catalog = zcatalog._catalog
            catalog.addIndex(id, obj)

            zcatalog._p_jar.add(obj)

            self.request.response.redirect(
                zcatalog.absolute_url() +
                "/manage_catalogIndexes?manage_tabs_message=Index%20Added")

        # Note the unfortunate homonym "index": self.index() renders the add
        # form, which submits to this method to add a catalog index.
        return self.index()

