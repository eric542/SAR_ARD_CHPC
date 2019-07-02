#!/usr/bin/env python

#####################################################################################
## Python script used to create and submit batch jobs on CSIRO HPC for processing 
## Sentinel backscatter data. Based on 's1fromNCI.py' from Fang Yuan @ GA on 
## opendatacube/radar GitHub.
##
## The list of possible input parameters (with typical examples) is provided below.
## All parameters are optional, unless otherwise specified.
##
##  --startdate 2018-01-01
##     Earliest date to search (inclusive), as yyyy-mm-dd. Non-optional parameter.
##  --enddate 2018-02-01
##     Latest date to search (NOT inclusive), as yyyy-mm-dd. Non-optional parameter.
##  --bbox 130.0 131.0 -21.0 -20.0
##     Lat/long bounding box to search within. Non-optional parameter.
##  --base_data_dir ./input_data_test
##     Base directory to download and save the raw / unprocessed data to (.zip files). 
#      Non-optional parameter.
##  --base_save_dir ./output_data_test
##     Base directory to save the processed data. Non-optional parameter.
##  --gpt_exec ./gpt
##     Path to a local GPT executable (possibly a symlink). Non-optional parameter.
##  --pixel_res 10.0
##     Pixel resolution in output product, in [m]. Default is 25.0 (DEF_PIXEL_RES in 
##     code below).
##   
##  --product SLC
##     Sentinel-1 SAR data product to search / process -- choices are 'SLC' or 'GRD'. 
##     Default is 'SLC'.
##  --mode EW
##     Required Sentinel-1 SAR data sensor mode -- choices are 'IW' and 'EW'. Default  
##     is 'IW'.
##  --polarisation VV
##     Required Sentinel-1 SAR data polarisation -- choices are 'HH', 'VV', 'HH+HV',  
##     'VH+VV'. Default will include any polarisations.
##  --orbitnumber 123
##     Search for Sentinel-1 SAR data in a relative orbit number. Default will include 
##     any orbit number.
##  --orbitdirection Descending
##     Search for Sentinel-1 SAR data in a specific orbit direction. Choices are 
##     'Ascending', 'Descending'. Default will include any orbit direction.
##  --verbose
##     Prints extra messages to screen. Default is not to print extra messages.
##  
##  --scenes_per_job 7
##     Maximum number of scenes to process per SBATCH job, to achieve sensible job 
##     resource requirements (walltime). Default is 10 (DEF_SCENES_PER_JOB in code 
##     below).
##  --jobs_basename ./test/job_543 
##     Base name or folder for the submitted SBATCH jobs. If it ends with '/' (i.e. a 
##     directory is provided), the default job name will be added to that path; the 
##     default name is 'dualpol_proc_YYYYMMDD_HHMMSS' (current date and time). If 
##     unused, the default 'jobs_basename' is set to the above default name.
## 
##  --submit_no_jobs
##     For debugging: use this flag to only display the SBATCH job commands, without 
##     actually submitting them to the HPC. Default is to submit SBATCH jobs.
##  --reprocess_existing
##     Use this flag to re-process scenes that already have existing output files in  
##     the save directory. Default is not to re-process existing data.
##  
## Examples:
##  > module load python3/3.6.1
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01 --jobs_basename /somedir/testing
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01 --jobs_basename ./subdir/
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-01-15 --scenes_per_job 3 --jobs_basename testing
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 147.0 148.3 -33.8 -33.0 --startdate 2016-08-01 --enddate 2016-12-01 --base_save_dir /data/abc123/Copernicus_Backscatter_Lachlan/ --submit_no_jobs
## Testing post-March'18 data:
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 147.0 148.3 -33.8 -33.0 --startdate 2018-06-01 --enddate 2018-06-15
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 147.0 148.3 -33.8 -33.0 --startdate 2018-02-01 --enddate 2018-02-15
## Benchmarking:
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 147.0 148.3 -33.8 -33.0 --startdate 2016-08-01 --enddate 2016-08-16
## 
## Production -- South East Victoria
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 146.5 147.5 -38.2 -37.8 --startdate 2018-01-01 --enddate 2018-07-01 --jobs_basename ./log/ --base_save_dir /data/abc123/Copernicus_Backscatter_SEVic/
## Production -- North West Victoria
##  > python3.6 CHPC_backsc_proc_qsub.py --bbox 142.3 144.0 -35.75 -34.5 --startdate 2018-01-01 --enddate 2018-07-01 --jobs_basename ./log/ --base_save_dir /data/abc123/Copernicus_Backscatter_NWVic/
#####################################################################################


import os, sys
import requests
from urllib.request import urlretrieve
import argparse
from datetime import datetime
import numpy as np
try:
    from urllib import quote as urlquote # Python 2.X
except ImportError:
    from urllib.parse import quote as urlquote # Python 3+


SARAQURL = "https://copernicus.nci.org.au/sara.server/1.0/api/collections/S1/search.json?"

JOB_SCRIPT = "CHPC_backsc_proc.sh"  # name of SBATCH shell script to carry out the backscatter processing
XML_GRAPH = "CHPC_backsc_proc_graph.xml"            # for consistency checks only -- as defined in JOB_SCRIPT
DEF_PIXEL_RES = "25.0"      # string, default pixel resolution in output product (in [m])

SOURCE_URL = "http://dapds00.nci.org.au/thredds/fileServer/fj7/Copernicus/"      # "hard-coded" url to the Sentinel-1 data on NCI Thredds server (ends with '/')
SOURCE_SUBDIR = "Sentinel-1"                # "hard-coded" next subdir in the source path (no trailing '/')

DEF_SCENES_PER_JOB = 10     # default nr of scenes per job submitted to SBATCH
WALLTIME_PER_SCENE = 20     # in [min]; estimate of required walltime to process one scene on 'N_CPUS'
N_CPUS = 8                  # number of cpus to use for each SBATCH job
MEM_REQ = 88                # in [GB]; MEM (RAM) requirements for SBATCH job
MAX_N_JOBS = 300

# Note on MEM_REQ value: the SNAP software on the HPC is typically installed with a definition of the maximum usable 
# MEM allocation (see -Xmx value in the gpt.vmoptions file). This means that the SBATCH jobs must be submitted with a 
# minimum of that amount of MEM. It is further suggested to have the max MEM value (-Xmx) in SNAP set to ~75% of the 
# total amount of RAM in the system. According to this, with e.g. -Xmx65GB, the SBATCH jobs should theoretically be 
# submitted with ~88GB of MEM.

def quicklook_to_url(qlurl):
    fp = SOURCE_URL + SOURCE_SUBDIR + qlurl.split(SOURCE_SUBDIR)[1].replace(".png",".zip")
    return fp

    
def main():
    # basic input checks:
    if not os.path.isfile(JOB_SCRIPT): sys.exit("Error: job script '%s' does not exist." % JOB_SCRIPT)
    if not os.path.isfile(XML_GRAPH): sys.exit("Error: XML graph file '%s' does not exist." % XML_GRAPH)
    
    
    # input parameters:
    parser = argparse.ArgumentParser(description="Backscatter processing: create and submit batch jobs on the NCI for processing Sentinel scenes to ARD data.")
    
    # non-optional parameters:
    parser.add_argument("--startdate", default=None,
                        help="Earliest date to search, as yyyy-mm-dd. Non-optional parameter.")
    parser.add_argument("--enddate", default=None,
                        help="Latest date to search, as yyyy-mm-dd. Non-optional parameter.")
    parser.add_argument("--bbox", nargs=4, type=float, default=None,
                        metavar=('westLong', 'eastLong', 'southLat', 'northLat'),
                        help=("Lat/long bounding box to search within. Non-optional parameter."))
    parser.add_argument( "--base_save_dir", default=None,
                         help="Base directory to save processed data. Default is %(default)s." )
    parser.add_argument( "--base_data_dir", default=None,
                         help="Base directory to save unprocessed data. Default is %(default)s." )
    parser.add_argument( "--gpt_exec", default=None,
                         help="Path to local GPT executable (possibly a symlink). Default is to load GPT from the SNAP module." )
    
    # optional parameters:
    parser.add_argument( "--pixel_res", default=DEF_PIXEL_RES, 
                         help="Pixel resolution in output product, in [m]. Default is %(default)s." )
    
    parser.add_argument( "--product", choices=['SLC','GRD'], default='GRD',
                         help="Data product to search. Default is %(default)s." )
    parser.add_argument( "--mode", choices=['IW','EW'], default='IW',
                         help="Required sensor mode. Default is %(default)s." )
    parser.add_argument( "--polarisation", choices=['HH', 'VV', 'HH+HV', 'VH+VV'],
                         help="Required polarisation. Default will include any polarisations." )
    parser.add_argument( "--orbitnumber", default=None, type=int,
                         help="Search in relative orbit number. Default will include any orbit number." )
    parser.add_argument( "--orbitdirection", choices=['Ascending', 'Descending'], default=None,
                         help="Search in orbit direction. Default will include any orbit direction." )
    parser.add_argument( "--verbose", action='store_true',
                         help="Prints messages to screen. Default is %(default)s.")
    
    parser.add_argument( "--scenes_per_job", default=DEF_SCENES_PER_JOB, type=int,
                         help="Maximum number of scenes to process per SBATCH job. Default is %(default)s." )
    parser.add_argument( "--jobs_basename", 
                         help="Base name or dir for submitted SBATCH jobs. If ends with '/' (i.e. directory), default name will be added to the path. Default name is 'backsc_proc_YYYYMMDD_HHMMSS' (current date and time)." )
    parser.add_argument( "--submit_no_jobs", action='store_true', 
                         help="Debug: use to only display job commands. Default is %(default)s." )
    parser.add_argument( "--reprocess_existing", action='store_true', 
                         help="Re-process already processed scenes with existing output files. Default is %(default)s." )
    
    
    # parse options:
    cmdargs = parser.parse_args()
    
    if cmdargs.startdate is None or cmdargs.enddate is None or cmdargs.bbox is None or cmdargs.base_save_dir is None or cmdargs.base_data_dir is None or cmdargs.gpt_exec is None:
        sys.exit("Error: Input arguments 'startdate', 'enddate', 'base_save_dir', 'base_data_dir', 'gpt_exec' and 'bbox' must be defined.")
    
    tmp = 'backsc_proc_' + str(datetime.now()).split('.')[0].replace('-','').replace(' ','_').replace(':','')
    if cmdargs.jobs_basename is None:
        cmdargs.jobs_basename = tmp
    elif cmdargs.jobs_basename.endswith("/"): 
        if not os.path.isdir(cmdargs.jobs_basename): os.mkdir(cmdargs.jobs_basename)
        cmdargs.jobs_basename += tmp
    
    if not cmdargs.base_save_dir.endswith("/"): cmdargs.base_save_dir += "/"
    if not cmdargs.base_data_dir.endswith("/"): cmdargs.base_data_dir += "/"
    
    # user provided path to local gpt exec
    if not os.path.isfile(cmdargs.gpt_exec):    # OK with symlinks
        sys.exit("Error: GPT executable '%s' does not exist." % cmdargs.gpt_exec)
    if not os.path.realpath(cmdargs.gpt_exec).endswith('gpt'):      # account for possible symlink
        sys.exit("Error: GPT executable '%s' does not point to executable named 'gpt'." % cmdargs.gpt_exec)
    
    
    # construct search url:
    queryUrl=SARAQURL
    if cmdargs.product:
        queryUrl += "&productType={0}".format(urlquote(cmdargs.product))
    if cmdargs.mode:
        queryUrl += "&sensorMode={0}".format(urlquote(cmdargs.mode))
    if cmdargs.startdate:
        queryUrl += "&startDate={0}".format(urlquote(cmdargs.startdate))
    if cmdargs.enddate:
        queryUrl += "&completionDate={0}".format(urlquote(cmdargs.enddate))
    if cmdargs.polarisation:
        queryUrl += "&polarisation={0}".format(urlquote(','.join(cmdargs.polarisation.split('+'))))
    if cmdargs.orbitnumber:
        queryUrl += "&orbitNumber={0}".format(urlquote('{0}'.format(cmdargs.orbitnumber)))
    if cmdargs.orbitdirection:
        queryUrl += "&orbitDirection={0}".format(urlquote(cmdargs.orbitdirection))
    if cmdargs.bbox:
        (westLong, eastLong, southLat, northLat) = cmdargs.bbox
        bboxWkt = 'POLYGON(({left} {top}, {right} {top}, {right} {bottom}, {left} {bottom}, {left} {top}))'.format(left=westLong, right=eastLong, top=northLat, bottom=southLat )
        queryUrl += "&geometry={0}".format(urlquote(bboxWkt))
    
    # make a paged SARA query:
    fileURLs = []
    queryUrl += "&maxRecords=50"
    page = 1
    if cmdargs.verbose: print(queryUrl)

    r = requests.get(queryUrl)
    result = r.json()
    nresult = result["properties"]["itemsPerPage"]
    while nresult>0:
        if cmdargs.verbose:
            print("Returned {0} products in page {1}.".format(nresult, page))

        # extract list of product URLs
        fileURLs += [quicklook_to_url(i["properties"]["quicklook"]) for i in result["features"]]
            
        # go to next page until nresult=0
        page += 1
        pagedUrl = queryUrl+"&page={0}".format(page)
        r = requests.get(pagedUrl)
        result = r.json()
        nresult = result["properties"]["itemsPerPage"]

    # final list of products:
    fileURLs = [ii for ii in fileURLs if ii is not None]
    
    # if needed, exclude .zip files already processed:
    if not cmdargs.reprocess_existing:
        tmp = len(fileURLs)
        fileURLs = [f for f in fileURLs if not os.path.isfile(f.replace(SOURCE_URL,cmdargs.base_save_dir).replace('.zip','.dim'))]
        nproc = tmp - len(fileURLs)
        if nproc!=0:
            print("A total of %i scenes (of %i) were found to be already processed (not re-processing)." % (nproc,tmp) )
    
    n_scenes = len(fileURLs)
    if n_scenes==0: 
        print("Found no (new) scene to process.")
        return
    
    
    # download zip files if not already available:
    filepaths = []
    for urli in fileURLs:
        fpath = urli.replace(SOURCE_URL,cmdargs.base_data_dir)
        filepaths += [fpath]
        if not os.path.isfile( fpath ):
            print( "Downloading .zip file: %s" % urli )
            os.makedirs( os.path.dirname(fpath), exist_ok=True)    # create path if necessary
            urlretrieve(urli, fpath)
    
    
    # write separate lists of scenes (one per SBATCH job):
    n_jobs = np.ceil( float(n_scenes) / cmdargs.scenes_per_job )
    if n_jobs>MAX_N_JOBS: sys.exit('Error: Too many SBATCH jobs for this query.')
    jobs_arr = np.array_split( filepaths, n_jobs )
    
    # write lists:
    ind = 0
    for job in jobs_arr:        
        ind += 1
        slist_name = cmdargs.jobs_basename + '_%03i.list' % ind
        with open(slist_name,'w') as ln:
            ln.writelines( map(lambda x: x + '\n', job) )
    
    
    # create SBATCH job scripts & submit to HPC:
    jlist_name = cmdargs.jobs_basename + '.jobs'
    with open(jlist_name,'w') as ln:
        ln.write( "\nBatch jobs for BACKSCATTER processing of SAR scenes\n" )
        ln.write( "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n" )
        ln.write( "Time is: %s \n\n" % str( datetime.now() ) )
        ln.write( "Input parameters are:\n" )
        ln.write( "  Start date: %s \n" % cmdargs.startdate )
        ln.write( "  End date: %s \n" % cmdargs.enddate )
        ln.write( "  Bounding box: %s \n" % str(cmdargs.bbox) )
        ln.write( "  Base save dir: %s \n" % cmdargs.base_save_dir )
        ln.write( "  Base data dir: %s \n" % cmdargs.base_data_dir )
        ln.write( "  GPT exec path: %s \n\n" % cmdargs.gpt_exec )
        ln.write( "Current directory is:\n  %s \n\n" % os.getcwd() )
        ln.write( "Submitted jobs are:\n" )
    
    ind = 0
    for job in jobs_arr:    # submit SBATCH job
        ind += 1
        slist_name = cmdargs.jobs_basename + '_%03i' % ind
        walltime = WALLTIME_PER_SCENE * len(job)
        
        sbstr = "--time=%i" % walltime
        sbstr += " --ntasks-per-node=%i" % N_CPUS
        sbstr += " --mem=%iGB" % MEM_REQ
        sbstr += " --output=%s" % (slist_name + '.out')
        sbstr += " --error=%s" % (slist_name + '.err')
        
        exstr = "ARG_FILE_LIST=%s" % (slist_name + '.list')
        exstr += ",BASE_SAVE_DIR=%s" % cmdargs.base_save_dir
        exstr += ",PIX_RES=%s" % cmdargs.pixel_res
        exstr += ",GPT_EXEC=%s" % cmdargs.gpt_exec
        sbstr += " --export=%s" % exstr
        
        cmd = 'sbatch ' + sbstr + ' ' + JOB_SCRIPT 
        cmdstr = 'Job nr. %03i of '%ind + str(int(n_jobs)) + ': ' + cmd
        print( "\n" + cmdstr )
        with open(jlist_name,'a') as ln:
            ln.write( "\n  " + cmdstr + "\n" )
        if not cmdargs.submit_no_jobs: os.system( cmd )
    
    
if __name__ == "__main__":
    main()

