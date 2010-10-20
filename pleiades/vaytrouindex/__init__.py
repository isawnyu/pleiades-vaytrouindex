from Products.CMFCore import utils 

from Products.Archetypes import atapi
from Products.ATContentTypes import permission

def initialize(context):
    from pleiades.vaytrouindex import criteria
    criteria # import to register

    listOfTypes = atapi.listTypes('pleiades.vaytrouindex')

    content_types, constructors, ftis = atapi.process_types(
        listOfTypes,
        'pleiades.vaytrouindex')

    allTypes = zip(content_types, constructors)
    for atype, constructor in allTypes:
        kind = "%s: %s" % (
            'pleiades.vaytrouindex', atype.archetype_name)
        utils.ContentInit(
            kind, content_types=(atype,),
            permission=permission.AddTopics,
            extra_constructors=(constructor,)).initialize(context)
