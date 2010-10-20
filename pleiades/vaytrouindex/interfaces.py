
import Globals  # import Zope 2 dependencies in order

from zope.interface import Attribute
from zope.interface import Interface
from Products.PluginIndexes.interfaces import IPluggableIndex


class IVaytrouIndex(IPluggableIndex):
    """A ZCatalog multi-index that uses Vaytrou for storage and queries."""
    vaytrou_uri = Attribute("The URI of the Vaytrou server")
    connection_manager = Attribute("""
        An IHTTPConnectionManager that is specific to the ZODB connection.
        """)


class IVaytrouConnectionManager(Interface):
    """Provides a Vaytrou connection and transaction integration.

    An instance of this class gets stored in the foreign_connections
    attribute of a ZODB connection.
    """
    connection = Attribute("An instance of httplib2.Http")
    #schema = Attribute("An ISolrSchema instance")
    vaytrou_uri = Attribute("The URI of the Vaytrou server")

    def set_changed():
        """Adds the Solr connection to the current transaction.

        Call this before sending change requests to Vaytrou.
        """
