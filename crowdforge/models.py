from django.db import models

class FlowType(models.Model):
    """Represents a model for how to solve a problem"""
    name = models.CharField(max_length=100)
    
    def __unicode__(self):
        return self.name

class HitType(models.Model):
    """
    Represents a type of HIT on Mechanical Turk. 
    For example, this can be an arch or a work HIT.
    
    This is a prototype for Hit
    """
    # templated parameters
    title = models.CharField(max_length=100)
    description = models.CharField(max_length=200)
    body = models.TextField()
    # some basic and necessary fields
    keywords = models.CharField(max_length=200)
    max_assignments = models.IntegerField(default=1)
    payment = models.FloatField(default=0.05)
    duration = models.IntegerField(default=30)
    approval_delay = models.IntegerField(default=60*3)
    lifetime = models.IntegerField(default=60*24)
    
    def __unicode__(self):
        return self.title

class Problem(models.Model):
    """
    Represents a problem to solve using CrowdForge
    """
    # base parameters for a problem
    name = models.CharField(max_length=100)
    stage = models.ForeignKey(HitType, related_name='problem_stage', blank=True, null=True)
    
    is_active = models.BooleanField(default=True)
    
    # core parts of a problem need to exist
    flow = models.ForeignKey(FlowType)
    partition = models.ForeignKey(HitType, related_name='problem_partition')
    mapper = models.ForeignKey(HitType, related_name='problem_mapper')
    reducer = models.ForeignKey(HitType, related_name='problem_reducer')
    
    # optional parts of a problem that might not be needed
    # for 2D partitions
    partition2 = models.ForeignKey(HitType, related_name='problem_partition2', blank=True, null=True)
    
    # verification steps
    partition_verify = models.ForeignKey(HitType, related_name='problem_partition_verify', blank=True, null=True)
    mapper_verify = models.ForeignKey(HitType, related_name='problem_mapper_verify', blank=True, null=True)
    reducer_verify = models.ForeignKey(HitType, related_name='problem_reducer_verify', blank=True, null=True)
    
    def __unicode__(self):
        return self.name
        
    def get_flow(self):
        flow_manager.get()

class Hit(models.Model):
    """
    Represents a HIT on Mechanical Turk.
    This row/object is 1:1 with an MTurk HIT
    """
    # the HIT ID on MTurk
    hit_id = models.CharField(max_length=100)
    hit_type = models.ForeignKey(HitType)
    problem = models.ForeignKey(Problem)
    # extra data to identify the HIT
    params = models.TextField(blank=True)
    # optional fields for overriding title and description
    title = models.TextField()
    description = models.TextField()
    body = models.TextField()
    
    is_active = models.BooleanField(default=True)
        
    @models.permalink
    def get_absolute_url(self):
        return ('crowdforge.views.hit', [str(self.id)])
        
    def __unicode__(self):
        return self.title[:20] + '... #' + self.hit_id
    
class Result(models.Model):
    """
    Represents the result of a HIT on Mechanical Turk.
    There can be many results for a single MTurk HIT
    """
    # the assignment ID on MTurk
    assignment_id = models.CharField(max_length=100)
    hit = models.ForeignKey(Hit)
    # JSON data for the result value
    value = models.TextField()
    created = models.DateTimeField(auto_now_add=True)
    
    def __unicode__(self):
        return 'Result for \"' + str(self.hit) + '\"'
        
    @models.permalink
    def get_absolute_url(self):
        return ('crowdforge.views.result', [str(self.id)])
