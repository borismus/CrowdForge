from django.conf.urls.defaults import *

urlpatterns = patterns('crowdforge.views',
    (r'^hit/(?P<id>\d+)/$', 'hit'),
    (r'^problem/(?P<id>\d+)/$', 'problem'),
    (r'^result/(?P<id>\d+)/$', 'result'),
)
