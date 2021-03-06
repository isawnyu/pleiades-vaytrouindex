from AccessControl import ClassSecurityInfo
from Products.Archetypes.atapi import DisplayList
from Products.Archetypes.atapi import FloatField, DecimalWidget
from Products.Archetypes.atapi import IntegerField
from Products.Archetypes.atapi import Schema
from Products.Archetypes.atapi import SelectionWidget
from Products.Archetypes.atapi import StringField, StringWidget
from Products.ATContentTypes import ATCTMessageFactory as _
from Products.ATContentTypes.criteria import registerCriterion
from Products.ATContentTypes.criteria.base import ATBaseCriterion
from Products.ATContentTypes.criteria.schemata import ATBaseCriterionSchema
from Products.ATContentTypes.interfaces import IATTopicSearchCriterion
from Products.ATContentTypes.permission import ChangeTopics
from Products.CMFCore.permissions import View
from Products.validation import validation
from Products.validation.validators.RegexValidator import RegexValidator
from zope.interface import implementer


isPoint = RegexValidator('isPoint',
                   r'^([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?,([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$',
                   title='', description='',
                   errmsg='is not a pair of decimal numbers.')
validation.register(isPoint)

GeoPredicates = DisplayList((
                    ('intersection', _(u'Intersection')),
                    ('distance', _(u'Distance')),
                    ('nearest', _(u'Nearest')),
#                  , ('min', _(u'Greater than'))
#                  , ('max', _(u'Less than'))
#                  , ('min:max', _(u'Between'))
    ))

GeolocationCriterionSchema = ATBaseCriterionSchema + Schema((
    StringField('lowerLeft',
                required=1,
                mode="rw",
                write_permission=ChangeTopics,
                default='',
                widget=StringWidget(
                    label=_(
                        u'label_geolocation_criteria_lowerleft', 
                        default=u'Lower left corner'),
                    description=_(
                        u'help_geolocation_criteria_lowerleft',
                        default=u'Comma separated decimal longitude and latitude values such as (for Rome): 12.5,41.9.')
                    ),
                validators=('isPoint',),
                ),
    StringField('upperRight',
                required=0,
                mode="rw",
                write_permission=ChangeTopics,
                default='',
                widget=StringWidget(
                    label=_(
                        u'label_geolocation_criteria_upperright', 
                        default=u'Upper right corner'),
                    description=_(
                        u'help_geolocation_criteria_upperright',
                        default=u'Optional parameter which combines with the above to form a search box.')                    
                    ),
                validators=('isPoint',),
                ),
    StringField('predicate',
                required=1,
                mode="rw",
                write_permission=ChangeTopics,
                default='intersection',
                vocabulary=GeoPredicates,
                enforceVocabulary=1,
                widget=SelectionWidget(
                    label=_(u'label_geolocation_criteria_predicate', default=u'Predicate'),
                    description=_(u'help_geolocation_criteria_predicate',
                                  default=u'Specify a geometric predicate.')
                    ),
                ),
    FloatField('tolerance',
                required=0,
                mode="rw",
                write_permission=ChangeTopics,
                default=0.0,
                widget=DecimalWidget(
                    size=30,
                    label=_(u'label_geolocation_criteria_tolerance', default=u'Distance Tolerance'),
                    description=_(u'help_geolocation_criteria_tolerance',
                                  default=u'Tolerance in kilometers for distance predicate')
                    ),
                ),
    IntegerField('limit',
                required=0,
                mode="rw",
                write_permission=ChangeTopics,
                default=1,
                widget=DecimalWidget(
                    size=4,
                    label=_(u'label_geolocation_criteria_limit', default=u'Nearest Limit'),
                    description=_(u'help_geolocation_criteria_limit',
                                  default=u'Maximum number of unique distances for nearest')
                    ),
                )
    ))


@implementer(IATTopicSearchCriterion)
class GeolocationCriterion(ATBaseCriterion):
    """A spatial search criterion"""

    security       = ClassSecurityInfo()
    schema         = GeolocationCriterionSchema
    meta_type      = 'GeolocationCriterion'
    archetype_name = 'Geolocation Criterion'
    shortDesc      = 'Spatial search criterion'

    security.declareProtected(View, 'Value')
    def Value(self):
        val = map(float, self.getLowerLeft().split(','))
        ur = self.getUpperRight()
        if ur:
            val += map(float, ur.split(','))
        return tuple(val)

    security.declareProtected(View, 'getCriteriaItems')
    def getCriteriaItems(self):
        result = []
        val = self.Value()
        predicate = self.getPredicate()
        if predicate == 'intersection':
            result.append(
                (self.Field(), {'query': val, 'range': predicate}))
        elif predicate == 'distance':
            result.append(
                (self.Field(), 
                 {'query': (val, self.getTolerance()*1000.0), 
                  'range': predicate}))
        elif predicate == 'nearest':
            result.append(
                (self.Field(), 
                 {'query': (val, self.getLimit()), 'range': predicate}))
        return tuple(result)

registerCriterion(GeolocationCriterion, ('VaytrouIndex', 'LocationQueryIndex'))
