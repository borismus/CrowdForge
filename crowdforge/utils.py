from boto.mturk.connection import MTurkConnection
from boto.mturk.question import ExternalQuestion
from crowdforge.models import Hit, Result

from django.utils import simplejson as json
import settings

def create_hit(problem, hit_type, params={}):
    """Utility method for creating a new HIT on AMT"""
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
    """Poll AMT for new results for the specified HIT"""
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
    """Poll AMT to check if the specified HIT is expired"""
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
    """Poll AMT to check if all instances of the specified HIT have been completed"""
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

