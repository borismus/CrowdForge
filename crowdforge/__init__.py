from boto.mturk.connection import MTurkConnection
from boto.mturk.question import ExternalQuestion

from django.utils import simplejson as json

import settings
from crowdforge.models import *

def create_hit(hit_type, problem, params={}):
    # create a Hit object
    hit = Hit(hit_id='?', hit_type=hit_type, problem=problem, params=json.dumps(params),
            title=hit_type.title%params, description=hit_type.description%params, body=hit_type.body%params)
    hit.save()
    
    # post a HIT on Mechanical Turk using boto
    q = ExternalQuestion(external_url=settings.URL_ROOT + hit.get_absolute_url(), frame_height=800)
    conn = MTurkConnection(aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                          host=settings.AWS_HOST)

    # remove commas from the keywords if they exist
    keywords=[k.replace(',', '') for k in hit_type.keywords.split()]
    create_hit_rs = conn.create_hit(question=q, lifetime=hit_type.lifetime, max_assignments=hit_type.max_assignments,
        keywords=keywords, reward=hit_type.payment, duration=hit_type.duration, approval_delay=hit_type.approval_delay, 
        title=hit.title, description=hit.description, annotation=`hit_type`)
    assert(create_hit_rs.status == True)
    
    # set the new HIT ID to be the hit_id for the new row.
    hit.hit_id = create_hit_rs.HITId
    hit.save()
    
    return hit

def fetch_results(hit):
    # check mturk for new results for this particular HIT
    conn = MTurkConnection(aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                          host=settings.AWS_HOST)
    # using the HIT ID, check results
    results = []
    assignments = conn.get_assignments(hit.hit_id)
    # go through the assignments
    for ass in assignments:
        # if there's already a result for this assignment, skip it
        if Result.objects.filter(assignment_id=ass.AssignmentId):
            continue
        # parse out the result
        data = {}
        for answer in ass.answers[0]:
            data[answer.QuestionIdentifier] = answer.FreeText
        
        # create new Result objects for each of them
        result = Result(assignment_id=ass.AssignmentId, hit=hit, value=json.dumps(data))
        result.save()
        results.append(result)
        
    return results
    
def is_expired(hit):
    conn = MTurkConnection(aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                          host=settings.AWS_HOST)
    result = conn.get_hit(hit.hit_id)[0]
    if hasattr(result, 'Error'):
        print "Something went wrong! %s is an invalid HIT" % str(hit)
        return True 
    assignments = conn.get_assignments(hit.hit_id)
    
    if result.expired:
        hit.is_active = False
        hit.save()
        return True
        
    return False
    
def is_complete(hit):
    conn = MTurkConnection(aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                          host=settings.AWS_HOST)
    result = conn.get_hit(hit.hit_id)[0]
    if hasattr(result, 'Error'):
        print "Something went wrong! %s is an invalid HIT" % str(hit)
        return True 
    assignments = conn.get_assignments(hit.hit_id)

    if int(result.MaxAssignments) == len(assignments):
        hit.is_active = False
        hit.save()
        return True

    return False

# method to check if we're ready to move on to the next step:
# if we're in the partition step, we're ready to move on when all partition type HITs for the problem are complete

def do_partition(problem):
    # creates HIT for the partitioning
    create_hit(problem.partition, problem)
    # if there's two partition functions, make both!
    if problem.partition2:
        create_hit(problem.partition2, problem)

def can_map(problem):
    # get all partition HITs for this problem
    hits = Hit.objects.filter(problem=problem, hit_type=problem.partition)
    # ensure that all of the partition HITs are completed or expired
    for hit in hits:
        if not is_expired_or_complete(hit):
            # some partition HITs aren't done yet
            return False
            
    return True
    
def do_map(problem):
    assert can_map(problem)
    # gets the result of the partitioning work
    partition = get_partition(problem)
    # creates HITs for the mapping
    for part in partition:
        create_hit(problem.mapper, problem, params=part)
    
def can_reduce(problem):
    # ensures that mapping is complete
    hits = Hit.objects.filter(problem=problem, hit_type=problem.mapper)
    # ensure that all of the map HITs are completed or expired
    for hit in hits:
        if not is_expired_or_complete(hit):
            # some map HITs aren't done yet
            return False

    return hasattr(problem, 'reducer')

def do_reduce(problem):
    assert can_reduce(problem)
    # gets all of the map HITs
    map_hits = Hit.objects.filter(problem=problem, hit_type=problem.mapper)
    for hit in map_hits:
        # for each map HIT, get all results for it
        params = {'list': get_reduction_data(hit)}
        params.update(json.loads(hit.part))
        
        # creates HITs for the reduction based on those results
        # print "create reduce hit with params", params
        create_hit(problem.reducer, problem, params=params)
    
def results(problem):
    # ensures reduction is complete
    # gets reduction results
    pass
    
def did_finish(problem):
    # checks if reduction is complete
    hits = Hit.objects.filter(problem=problem, hit_type=problem.reducer)
    # ensure that all of the reduce HITs are completed or expired
    for hit in hits:
        if not is_expired_or_complete(hit):
            # some map HITs aren't done yet
            return False
            
    return True
    
def get_reduction_data(hit):
    # gets the payload for the reduce HIT.
    # we want it in an HTML list. <li>
    # reduce template: 
    list_html = ''
    results = hit.result_set.all()
    for result in results:
        value = json.loads(result.value)
        list_html += '<li>%s</li>' % value['fact']
    
    return list_html
    
def get_partition(problem):
    partition = []
    if not problem.partition2:
        # outline-style
        results = Result.objects.filter(hit__hit_type=problem.partition, hit__problem=problem)
        # just use the first one for now
        first_result = results[0]
        outline = json.loads(first_result.value).items()
        for item in sorted(outline):
            if item[1]:
                partition.append([item[1]])
    else:
        # table-style
        row_headers = Result.objects.filter(hit__hit_type=problem.partition, hit__problem=problem)
        col_headers = Result.objects.filter(hit__hit_type=problem.partition2, hit__problem=problem)
        # again, use the first of both for now
        first_row_header = row_headers[0]
        row_labels = json.loads(first_row_header.value).items()
        
        first_col_header = col_headers[0]
        col_labels = json.loads(first_col_header.value).items()
        for r in row_labels:
            for c in col_labels:
                if r[1] and c[1]:
                    partition.append([r[1], c[1]])
                
    indices = [str(i) for i in range(len(partition[0]))]
    return [dict(zip(indices, item)) for item in partition]
    