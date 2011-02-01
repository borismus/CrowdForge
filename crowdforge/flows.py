from django.utils import simplejson as json
from crowdforge.models import *
from crowdforge.utils import create_hit
from django.db.models import Q

class Flow():
    
    def __init__(self, problem):
        self.problem = problem
    
    def start(self):
        """First step in solving the problem"""
        pass
        
    def end(self):
        self.problem.is_active = False
        self.problem.save()
        
    # utility methods
    def create_hit(self, hit_type, params={}):
        print 'create hit', hit_type, 'params', params
        create_hit(self.problem, hit_type, params)
        
    def set_stage(self, stage):
        self.problem.stage = stage
        self.problem.save()
    
    # callbacks
    def on_hit_complete(self, hit):
        print "hit complete", hit

    def on_hit_expired(self, hit):
        print "hit expired", hit

    def on_results_retrieved(self, results):
        print "results retrieved", results
    
    def on_stage_completed(self, stage):
        pass

        
flows = {}
def register(name, flow):
    # check existing flows
    flows_with_same_name = FlowType.objects.filter(name=name)
    if not flows_with_same_name:
        new_flow = FlowType(name=name)
        new_flow.save()
        
    flows[name] = flow
    
def get(problem):
    return flows[problem.flow.name](problem)

class SimpleFlow(Flow):
    
    def start(self):
        self.set_stage(self.problem.partition)
        self.create_hit(self.problem.partition)

    def on_stage_completed(self, stage):
        print "completed stage, ", stage
        if stage == self.problem.partition:
            # get the partition
            partition = self.get_first_partition()
            # if the partition finished, make map HITs
            for part in partition:
                self.create_hit(self.problem.mapper, params={'topic': part})
            # and we're in the map stage
            self.set_stage(self.problem.mapper)

        elif stage == self.problem.mapper:
            # if the map finished, get all map hits
            map_hits = Hit.objects.filter(problem=self.problem, hit_type=self.problem.mapper)
            for hit in map_hits:
                # for each map HIT, get all results for it
                params = {'list': self.get_map_results(hit)}
                params.update(json.loads(hit.params))
                # creates HITs for the reduction based on those results
                self.create_hit(self.problem.reducer, params)

            # and we're in the reduce stage
            self.set_stage(self.problem.reducer)

        elif stage == self.problem.reducer:
            # finalize the whole thing
            self.end()
            
    # helpers
    def get_first_partition(self):
        # get all results to the partition for this problem
        results = Result.objects.filter(hit__problem=self.problem, hit__hit_type=self.problem.partition)
        # get the first and only result to the partition task
        return self.get_partition(results[0])
        
    def get_partition(self, result):
        partition = []
        outline = sorted(json.loads(result.value).items())
        return [item[1] for item in outline if item[1]]
        
    def get_map_results(self, hit):
        # gets the payload for the reduce HIT.
        # we want it in an HTML list. <li>
        # reduce template: 
        list_html = ''
        results = hit.result_set.all()
        for result in results:
            value = json.loads(result.value)
            list_html += '<li>%s</li>' % value['fact']

        return list_html
        
register('SimpleFlow', SimpleFlow)


class VerificationFlow(SimpleFlow):
    
    def on_stage_completed(self, stage):
        print "completed stage, ", stage
        if stage == self.problem.partition:
            # get the (only) partition HIT
            partition_results = Result.objects.filter(hit__problem=self.problem, 
                hit__hit_type=self.problem.partition).order_by('?')
            # get all formatted partition results in random order
            formatted_partitions = [self.get_formatted_partition(p) for p in partition_results]
            # make verification HITs to rate all of the partitions
            self.create_hit(self.problem.partition_verify, params={'partitions': ''.join(formatted_partitions)})
            # and we're in the partition verification stage
            self.set_stage(self.problem.partition_verify)
        
        elif stage == self.problem.partition_verify:
            # if the partition verify stage finished, collect all of the 
            # verification HITs.
            partition = self.get_top_rated_partition()
            # make map HITs
            for part in partition:
                self.create_hit(self.problem.mapper, params={'topic': part})
            # and we're in the map stage
            self.set_stage(self.problem.mapper)
            
        elif stage == self.problem.mapper:
            # if the map finished, get all map hits
            map_hits = Hit.objects.filter(problem=self.problem, hit_type=self.problem.mapper)
            for hit in map_hits:
                # for each map HIT, get all results for it
                params = {'list': self.get_map_results(hit)}
                params.update(json.loads(hit.params))
                # creates HITs for the reduction based on those results
                self.create_hit(self.problem.reducer, params)

            # and we're in the reduce stage
            self.set_stage(self.problem.reducer)

        elif stage == self.problem.reducer:
            # finalize the whole thing
            self.end()
            
    
    def get_formatted_partition(self, result):
        template = """<div class="partition"><ol>%(partition)s</ol><table><tr>
        <td><input type="radio" name="%(result_id)s" value="1" /></td> 
        <td><input type="radio" name="%(result_id)s" value="2" /></td>
        <td><input type="radio" name="%(result_id)s" value="3" /></td> 
        <td><input type="radio" name="%(result_id)s" value="4" /></td> 
        <td><input type="radio" name="%(result_id)s" value="5" /></td> 
        </tr><tr><td>1</td><td>2</td><td>3</td><td>4</td><td>5</td></tr></table></div>"""
        partition = self.get_partition(result)
        # now format the partition in HTML so that it can be placed in the form
        partition_list = ''.join(['<li>%s</li>' % part for part in partition])
        
        return template % {'partition': partition_list, 'result_id': result.id}
            
    def get_top_rated_partition(self):
        # assume there's just one partition HIT
        partition_hit = Hit.objects.get(problem=self.problem, hit_type=self.problem.partition_verify)
        results = partition_hit.result_set.all()
        # compute ratings for each of the partition results
        # dictionary of partition_id: [average_rating, number_of_ratings]
        ratings = {}
        
        # get all verification results
        for result in results:
            # get all ratings for each turker that verified
            user_ratings = json.loads(result.value)
            for partition_id in user_ratings:
                rating = user_ratings[partition_id]
                if not ratings.has_key(partition_id):
                    # if this partition hasn't been rated yet, add it
                    ratings[partition_id] = [rating, 1]
                else:
                    # otherwise, recompute the new average
                    current_rating, number_of_ratings = ratings[partition_id]
                    ratings[partition_id] = [(current_rating + rating)/(number_of_ratings + 1), 
                                             number_of_ratings + 1]
        
        # get the highest rated partition_id
        rating_items = ratings.items()
        rating_items.sort(lambda a, b: cmp(b[1][0], a[1][0]))
        top_partition_id = rating_items[0][0]
        
        # return the partition object
        return self.get_partition(Result.objects.get(pk=top_partition_id))
        
register('VerificationFlow', VerificationFlow)

class PartitionSelectionExperimentFlow(SimpleFlow):
    # goes partition -> reduce -> map (actually a vote)
    

    PARTITION_RATE_TEMPLATE = """<div class="partition"><ol>%(partition)s</ol><table><tr>
    <td><input type="radio" name="%(result_id)s" value="1" /></td> 
    <td><input type="radio" name="%(result_id)s" value="2" /></td>
    <td><input type="radio" name="%(result_id)s" value="3" /></td> 
    <td><input type="radio" name="%(result_id)s" value="4" /></td> 
    <td><input type="radio" name="%(result_id)s" value="5" /></td> 
    </tr><tr><td>1</td><td>2</td><td>3</td><td>4</td><td>5</td></tr></table></div>"""
    
    PARTITION_REDUCE_TEMPLATE = """<div class="partition"><ol>%(partition)s</ol></div>"""
    
    def start(self):
        self.set_stage(self.problem.partition)
        self.create_hit(self.problem.partition)
        
    def on_hit_complete(self, hit):
        print "hit complete", hit

    def on_hit_expired(self, hit):
        print "hit expired", hit

    def on_results_retrieved(self, results):
        print "results retrieved", results
    
    def on_stage_completed(self, stage):
        if stage == self.problem.partition:
            # get all partitions that were submitted
            partition_results = Result.objects.filter(hit__problem=self.problem, 
                hit__hit_type=self.problem.partition).order_by('?')
            formatted_partitions = [self.get_formatted_partition(p, PartitionSelectionExperimentFlow.PARTITION_REDUCE_TEMPLATE) 
                for p in partition_results]
                
            # make a reduce HIT with all partitions shown and a place to input a new partition.
            self.create_hit(self.problem.reducer, params={'partitions': ''.join(formatted_partitions)})
            
            # once partition is complete, move to reduce
            self.set_stage(self.problem.reducer)
        
        if stage == self.problem.reducer:
            # get all partitions that were submitted and reduced
            partition_results = Result.objects.filter(Q(hit__hit_type=self.problem.partition) | Q(hit__hit_type=self.problem.reducer), hit__problem=self.problem).order_by('?')
            formatted_partitions = [self.get_formatted_partition(p, PartitionSelectionExperimentFlow.PARTITION_RATE_TEMPLATE) 
                for p in partition_results]
                
            # make a reduce HIT with all partitions shown and a place to input a new partition.
            self.create_hit(self.problem.mapper, params={'partitions': ''.join(formatted_partitions)})
            
            # once partition is reduced, move to rating each partition
            self.set_stage(self.problem.mapper)
            
        if stage == self.problem.mapper:
            # if map stage is done, we're finished.
            self.end()
            
    def get_ratings(self):
        # returns all harvested partitions and their ratings: 
        # [{'partition': ['foo', 'bar', 'baz'], 'rating': 3.4, 'respondents': 5}, ...]
        vote_results = Result.objects.filter(hit__hit_type=self.problem.mapper)
        all_ratings = {}
        # go through all vote results
        for results in vote_results:
            ratings = json.loads(results.value)
            # each result contains ratings for multiple partitions
            for partition_id in ratings:
                new_rating = int(ratings[partition_id])
                # if the partition already has a rating
                if all_ratings.has_key(partition_id):
                    curr = all_ratings[partition_id]
                    respondents = curr['respondents']
                    current_rating = curr['rating']
                    curr['respondents'] += 1
                    curr['rating'] = float(new_rating + respondents*current_rating)/(respondents + 1)
                else:
                    partition = Result.objects.get(pk=partition_id)
                    all_ratings[partition_id] = {
                        'partition': self.get_partition(partition),
                        'respondents': 1,
                        'rating': new_rating,
                        'origin': partition.hit.hit_type == self.problem.partition and 'original' or 'reduced',
                    }
                
        return all_ratings.values()
    
    def get_formatted_partition(self, result, template):
        partition = self.get_partition(result)
        # now format the partition in HTML so that it can be placed in the form
        partition_list = ''.join(['<li>%s</li>' % part for part in partition])

        return template % {'partition': partition_list, 'result_id': result.id}
        
        
        
register('PartitionSelectionExperimentFlow', PartitionSelectionExperimentFlow)