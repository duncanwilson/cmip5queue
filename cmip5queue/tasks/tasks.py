from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
from shutil import copyfile, move
import requests
import os
import json as jsonx

@task()
def cmip5_ncrcat(args):
    """
        AVAILABLE ON "SLOW" QUEUE ONLY
        Runs an R script to process CMIP5 data
        args: run parameters saved as a text file in json format
              The run parameters will be read into R as part of the R script
    """
    task_id = str(cmip5_ncrcat.request.id)
    user_id = args['user_name']
    resultDir = setup_result_directory(user_id, task_id)
    with open(resultDir + '/input/runargs.json', "w") as f:
        jsonx.dump(args,f)
    #Run R Script
    r_return = subprocess.call("Rscript --vanilla /sccsc/cmip5_ncrcat.R {0} {1}".format(user_id, task_id), shell=True)
    
    # read in R flags (written by R as part of run)
    with open(resultDir + '/flags.json') as json_flags:
        flags = jsonx.load(json_flags)
    final_pass = flags['final_result']
    if final_pass:
        result_msg = 'FAIL: There appeared to be an error in the run. Check error_flags.log for details.'
    else:
        result_msg = 'PASS: There were no error flags in the run'    
    return {"pass_result ": result_msg,
            "result_url":"http://{0}/cmip5_tasks/{1}/{2}".format("climatedata.oscer.ou.edu",user_id,task_id)}
            	
def setup_result_directory(user_id, task_id):
    resultDir = os.path.join('/home/celery/', user_id, task_id)
    os.makedirs(resultDir)
    os.chmod(resultDir,0777)
    os.makedirs("{0}/input".format(resultDir))
    os.chmod("{0}/input".format(resultDir),0777)
    os.makedirs("{0}/output".format(resultDir))
    os.chmod("{0}/output".format(resultDir),0777)
    return resultDir 
