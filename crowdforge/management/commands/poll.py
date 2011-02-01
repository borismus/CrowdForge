from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
import os
import sys

from crowdforge.models import *
from crowdforge.utils import fetch_results, is_expired, is_complete
from crowdforge import flows

class Command(BaseCommand):
    help='solve problems'
    
    def handle(self, **options):
        # run through AMT data and post any necessary notifications
        self.post_notifications()
        
    def post_notifications(self):
        """
        Generates up to four notifications to be handled by the problem's Flow object
        1. result retrieved
        2. hit expired
        3. hit complete
        4. stage complete
        """
        # go through active hits
        active_hits = Hit.objects.filter(is_active=True)
        
        # go through active hits to check if there are any new results.
        for hit in active_hits:
            results = fetch_results(hit)
            if results:
                flow = flows.get(hit.problem)
                # post notifications (results retrieved)
                flow.on_results_retrieved(results)

                if is_expired(hit):
                    # post notifications (hit expired)
                    flow.on_hit_expired(hit)
                elif is_complete(hit):
                    # post notifications (hit complete)
                    flow.on_hit_complete(hit)
        
        # go through active problems 
        active_problems = Problem.objects.filter(is_active=True)
    
        for problem in active_problems:
            flow = flows.get(problem)
            # if no stage, problem isn't being solved yet
            if not problem.stage:
                # start the flow
                flow.start()
                continue
            
            # fetch all currently active hits for this stage of the problem again
            active_hits = Hit.objects.filter(problem=problem, hit_type=problem.stage, is_active=True)
            
            # if there are no active hits
            if not active_hits:
                # post notifications (stage complete)
                flow.on_stage_completed(problem.stage)