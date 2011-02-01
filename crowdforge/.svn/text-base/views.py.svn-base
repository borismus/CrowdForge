from django.shortcuts import render_to_response, get_object_or_404
from django.http import Http404
from django.utils import simplejson as json

from models import Hit, Problem, Result
import settings

def hit(request, id):
    h = get_object_or_404(Hit, pk=id)
    # see if there's a turkSubmitTo parameter in the request GET:
    # action = 'http://www.mturk.com/mturk/externalSubmit'
    action = 'http://workersandbox.mturk.com/mturk/externalSubmit'
    if request.GET.has_key('turkSubmitTo'):
        action = request.GET['turkSubmitTo'] + '/mturk/externalSubmit'
    
    assignment_id = None
    if request.GET.has_key('assignmentId'):
        assignment_id = request.GET['assignmentId']
        
    return render_to_response('hit.html', 
        {'title': h.title, 'body': h.body, 'assignment_id': assignment_id, 'action': action})
        
def problem(request, id):
    p = get_object_or_404(Problem, pk=id)
    
    # get all HITs of a certain type associated with this problem
    partition_hits = p.partition.hit_set.filter(problem=p)
    partition2_hits = p.partition2 and p.partition2.hit_set.filter(problem=p) or None
    map_hits = p.mapper.hit_set.filter(problem=p)
    reduce_hits = p.reducer and p.reducer.hit_set.filter(problem=p) or None
    
    # get total number of HITs taken and total cost
    total_results = Result.objects.filter(hit__problem=p)
    number = len(total_results)
    cost = sum([result.hit.hit_type.payment for result in total_results])
    
    if number >= 2:
        # get the first and last created HITs here to see how long the whole thing took
        first = total_results.order_by('created')[0].created
        last = total_results.order_by('created').reverse()[0].created
        duration = last - first
    else:
        duration = None
    
    return render_to_response('problem.html', {'problem': p, 
            'partition': partition_hits, 'partition2': partition2_hits, 
            'map': map_hits, 'reduce': reduce_hits, 'number': number, 'cost': cost, 'duration': duration})
            
def result(request, id):
    r = get_object_or_404(Result, pk=id)
    return render_to_response('result.html', {'result': r, 'value_dict': json.loads(r.value)})