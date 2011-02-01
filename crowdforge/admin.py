from django.contrib import admin
from turk.crowdforge.models import *
# include all flows so that registration occurs.
from turk.crowdforge.flows import *

class ProblemAdmin(admin.ModelAdmin):
    exclude = ('stage', 'is_active')

admin.site.register(HitType)
admin.site.register(Problem, ProblemAdmin)