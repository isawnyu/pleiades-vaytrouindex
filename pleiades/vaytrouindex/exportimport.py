
from pleiades.vaytrouindex.interfaces import IVaytrouIndex
from Products.GenericSetup.interfaces import ISetupEnviron
from Products.GenericSetup.utils import NodeAdapterBase
from Products.GenericSetup.utils import PropertyManagerHelpers
from zope.component import adapts


class VaytrouIndexNodeAdapter(NodeAdapterBase, PropertyManagerHelpers):

    """GenericSetup node importer and exporter for VaytrouIndex.
    """

    adapts(IVaytrouIndex, ISetupEnviron)

    def _exportNode(self):
        """Export the object as a DOM node.
        """
        node = self._getObjectNode('index')
        node.appendChild(self._extractProperties())
        return node

    def _importNode(self, node):
        """Import the object from the DOM node.
        """
        if self.environ.shouldPurge():
            self._purgeProperties()
        self._initProperties(node)

        if node.hasAttribute('clear'):
            # Clear the index
            self.context.clear()

    node = property(_exportNode, _importNode)


class PlaceVaytrouIndexNodeAdapter(NodeAdapterBase, PropertyManagerHelpers):

    adapts(ISetupEnviron)

    def _exportNode(self):
        """Export the object as a DOM node.
        """
        node = self._getObjectNode('index')
        node.appendChild(self._extractProperties())
        return node

    def _importNode(self, node):
        """Import the object from the DOM node.
        """
        if self.environ.shouldPurge():
            self._purgeProperties()
        self._initProperties(node)

        if node.hasAttribute('clear'):
            # Clear the index
            self.context.clear()

    node = property(_exportNode, _importNode)

