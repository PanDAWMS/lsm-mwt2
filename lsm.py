#!/usr/bin/env python
#
# Importable module for lsm scripts
#

# The LSM Version
lsmversion = "3.00.1"

####################################################################################################
#
#
# LSM exit codes for the Pilot
#
#  http://www.usatlas.bnl.gov/twiki/bin/view/Admins/LocalSiteMover
#
#    0 - Transfer was successful
#
#  200 - GENERIC failure
#  201 - Copy command failed
#  202 - Unsupported command
#  203 - Unsupported option (e.g. Space token or checksum type)
#  204 - Size comparison failed
#  205 - Checksum comparison failed
#  206 - Unable to write to destination (destination does not exist)
#  207 - Unable to write to destination (permission problem)
#  208 - Overload (copy failed for overload, retry later)
#  209 - Size provided different from source file (this implies also 204)
#  210 - Checksum provided different from source file (this implies also 205)
#  211 - File already exist and is different (size/checksum).
#  212 - File already exist and is the same as the source (same size/checksum)
#  213 - Destination full (no space to write the output file)
#  220 - GENERIC transient failure (a suggestion for the pilot to retry later) 
#
#
####################################################################################################
#
#
# Logfile messages
#
#
# INFO
#
#    0 - Transfer command was successful
#    1 - Command line
#    2 - Transfer protocol: %s
#    3 - pCache   command : %s
#    4 - Transfer took %s seconds for a %s byte file,  %.2f b/s
#    5 - Local checksum took %s seconds for %s byte file
#    6 - Cached %s byte file
#    7 - Transfer command : %s
#    8 - Transfer command was successful but needed retries
#    9 - Spacetoken %s has %sMB free space
#   10 - Transfer   output: %s
#   11 - Verbosity enabled
#   12 - Removed file at %s
#   13 - Sleeping %s seconds before the next retry
#
#   20 - Exit to Pilot with code %s
#
# WARN
#
#   50 - Transfer command failed with copy status %s
#   51 - Transfer command timed out
#   52 - Transfer command exited successfully but destination file does not exist
#   53 - Size mismatch %s!=%s
#   54 - Checksum %s failed with: %s
#   55 - Checksum mismatch %s!=%s
#   56 - Ignoring transfer protocol %s because there is no available command
#   57 - Unable to remove file %s
#   58 - Failed to send to Elastic Search : %s
#   59 - Error in indexing in Elastic Search : %s
#   60 - Transfer command was not found
#   61 -
#   62 - Using source file size for destination: %s
#   63 - Using source file checksum for destination: %s
#   64 - Destination file exists but with a different checksum: %s, checksum: %s != %s
#   65 - Destination file exists but with a different size: %s size:%s != %s
#   66 - Destination file does not exist
#
#
# ERROR
#
#  200 - Unknown return code from pcache : %s
#  201 - Invalid command, SFN_ROOT %s not found" % SFN_ROOT
#  202 - Invalid command, needs at least SRC and DST
#  203 - Unsupported checksum type: %s
#  204 - File exists but with a different checksum: %s, checksum: %s != %s
#  205 - File exists but with a different size: %s size:%s != %s
#  206 - File exists and is identical to source: %s
#  207 - Cannot create output directory: %s
#  208 - Checksum %s failed with: %s
#  209 - Invalid command, illegal size specified
#  210 - Invalid command, unknown transport protocol specified: %s
#  211 - Invalid command, unknown argument given: %s
#  212 - Invalid command, a single storage endpoint must be specified
#  213 - Invalid command, SRC must begin with 'srm:// or httpg://'
#  214 - Invalid command, SRC must begin with 'srm://'
#  215 - Source file does not exist: %s
#  216 - Source file %s has an incorrect size: %s != %s
#  217 - Source file %s has an incorrect checksum: %s != %s
#  218 - Destination does not have enough free space
#  219 - Invalid command, source must end in a file name
#
#  254 - Internal error
#  255 - All transfer commands failed
#
#
####################################################################################################

import sys, os, stat, time, syslog
import zlib, hashlib
import subprocess, signal
import requests, json


from cStringIO import StringIO
from socket    import gethostname
from datetime  import datetime

from lsmsite   import *
import lsmsite


# Build a session ID for the logs
sessid="%s.%s" % ( int(time.time()), os.getpid() )

# Internal LSM codes to exit code for Pilot
mapLSMtoPilot = {}
mapLSMtoPilot[   0 ] =   0
mapLSMtoPilot[   1 ] =   0
mapLSMtoPilot[   2 ] =   0

mapLSMtoPilot[  50 ] = 201
mapLSMtoPilot[  51 ] = 201
mapLSMtoPilot[  52 ] = 206
mapLSMtoPilot[  53 ] = 209
mapLSMtoPilot[  54 ] = 205
mapLSMtoPilot[  55 ] = 210

mapLSMtoPilot[  60 ] = 200

mapLSMtoPilot[  64 ] = 211
mapLSMtoPilot[  65 ] = 211
mapLSMtoPilot[  66 ] = 212

mapLSMtoPilot[ 200 ] = 200
mapLSMtoPilot[ 201 ] = 202
mapLSMtoPilot[ 202 ] = 202
mapLSMtoPilot[ 203 ] = 203
mapLSMtoPilot[ 204 ] = 211
mapLSMtoPilot[ 205 ] = 211
mapLSMtoPilot[ 206 ] = 212
mapLSMtoPilot[ 207 ] = 206
mapLSMtoPilot[ 208 ] = 205
mapLSMtoPilot[ 209 ] = 205
mapLSMtoPilot[ 210 ] = 202
mapLSMtoPilot[ 211 ] = 202
mapLSMtoPilot[ 212 ] = 202
mapLSMtoPilot[ 213 ] = 202
mapLSMtoPilot[ 214 ] = 202
mapLSMtoPilot[ 215 ] = 200
mapLSMtoPilot[ 216 ] = 209
mapLSMtoPilot[ 217 ] = 210
mapLSMtoPilot[ 218 ] = 213
mapLSMtoPilot[ 219 ] = 202

mapLSMtoPilot[ 254 ] = 200
mapLSMtoPilot[ 255 ] = 201

mapLSMtoPilot[ 999 ] = 255


####################################################################################################

# Get host name (full and short)
hostFQDN          = gethostname()
hostName          = hostFQDN.split('.')[0]


# ElasticSearch Index - each script appends its "/type" such as "/get" or "/put"
esIndexBase       = 'http://atlas-kibana.mwt2.org:9200/lsm_6_%s.%s' % ( str(datetime.utcnow().year), str(datetime.utcnow().month) )


# Preload some fixed ElasticSearch values
esPayload               = {}
esPayload['sitename']   = siteName
esPayload['hostname']   = hostName
esPayload['lsmversion'] = lsmversion


####################################################################################################


def log(msg) :

  try :
    f=open(LOGFILE, 'a')
    f.write("%s %s %s\n" % (time.strftime("%F %H:%M:%S"), sessid, msg))
    f.close()
    os.chmod(LOGFILE, 0666)

  except Exception, e :
    pass

  ident=sys.argv[0].split('/')[-1]

  try : 
    syslog.openlog(ident)
    syslog.syslog("%s %s\n" % ( sessid, msg ) )

  except Exception, e :
    pass



def fail(errorcode=999,msg=None) :

  code = str(errorcode).zfill(3)

  if ( msg ) :
    msg = 'ERROR %s %s' % ( code, msg )
  else :
    msg = 'ERROR %s'    % ( code )

  print msg
  log(msg)

  sys.exit(errorcode)



def error(errorcode=999,msg=None) :

  code = str(errorcode).zfill(3)

  if ( msg ) :
    msg = 'ERROR %s %s' % ( code, msg )
  else :
    msg = 'ERROR %s'    % ( code )

# print msg
  log(msg)



def warn(errorcode=100,msg=None) :

  code = str(errorcode).zfill(3)

  if ( msg ) :
    msg = 'WARN  %s %s' % ( code, msg )
  else :
    msg = 'WARN  %s'    % ( code )

# print msg
  log(msg)



def info(errorcode=0,msg=None) :

  code = str(errorcode).zfill(3)

  if ( msg ) :
    msg = 'INFO  %s %s' % ( code, msg )
  else :
    msg = 'INFO  %s'    % ( code )

# print msg
  log(msg)


####################################################################################################


class Timer :
   def __init__(self):
     self.t0 = time.time()
   def __str__(self):
     return "%0.2f" % (time.time() - self.t0)
   def __float__(self):
     return time.time() - self.t0


####################################################################################################


class Capturing(list) :
    def __enter__(self) :
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout   = self._stringio = StringIO()
        sys.stderr   = sys.stdout
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout   = self._stdout
        sys.stderr   = self._stderr


####################################################################################################


# Run a command with a timeout in seconds

def RunCMD(cmd, timeout = 0 ) :

  class Alarm(Exception):
    pass

  def alarm_handler(signum, frame):
    raise Alarm


  # Execute the command as a subprocess
  try :
    p = subprocess.Popen(				\
        args   = cmd,					\
        shell  = True,					\
        stdout = subprocess.PIPE,			\
        stderr = subprocess.STDOUT			\
   )

  except :
    return ( -2, None )


  # Set the timer if a timeout value was given
  if ( timeout > 0 ) :
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(timeout)


  # Wait for the command to complete
  try:

    # Collect the output when the command completes
    stdout = p.communicate()[0][:-1]

    # Commmand completed in time, cancel the alarm
    if ( timeout > 0 ) : signal.alarm(0)


  # Command timed out
  except Alarm:

    # The pid of our spawn
    pids = [p.pid]

    # The pids of the spawn of our spawn
    pids.extend(ChildRunCMD(p.pid))

    # Terminate all of the evil spawn
    for pid in pids:
      try:
        os.kill(pid, signal.SIGKILL)
      except OSError:
        pass

      # Return a timeout error
      return ( -1, None )


  return (p.returncode, stdout)



def ChildRunCMD(pid):

  # Get a list of all pids associated with a given pid
  p = subprocess.Popen(                                 \
    args   = 'ps --no-headers -o pid --ppid %d' % pid,  \
    shell  = True,                                      \
    stdout = subprocess.PIPE,                           \
    stderr = subprocess.PIPE                            \
  )

  # Wait and fetch the stdout
  stdout, stderr = p.communicate()

  # Return a list of pids as tuples
  return [int(p) for p in stdout.split()]


####################################################################################################


def unitize(x):

  suff='BKMGTPEZY'

  while ( (x >= 1024) and suff ) :
    x /= 1024.0
    suff = suff[1:]
  return "%.4g%s" % (x, suff[0])


####################################################################################################


def exitToPilot(errorCode=999, errorMessage=None) :

  try :
    _errcode = mapLSMtoPilot[errorCode]
  except :
    _errcode = 255
    error(999,'Unmapped error code : %s' % errorCode)


  if ( errorMessage == None ) :
    pass
  else :
    print "%s" % errorMessage

  info( 20, 'Exit to Pilot with code %s' % _errcode )

  sys.exit(_errcode)


####################################################################################################


# Compute an ADLER32 checksum

def adler32(fname) :

  # Blocksize of the file
  BLOCKSIZE = 4096 * 1024

  # Open the file
  f = open(fname,'r')

  #Important - dCache uses seed of 1 not 0
  checksum = 1

  # Read until we run out of data adding to the checksum
  while True :
    data = f.read(BLOCKSIZE)
    if (not data) : break
    checksum = zlib.adler32(data, checksum)

  f.close()

  # Work around problem with negative checksum
  if (checksum < 0) : checksum += 2**32

  # Return with the computed checksum
  return hex(checksum)[2:10].zfill(8).lower()


####################################################################################################


# Compute an MD5 checksum

def md5sum(fname) :

  # Blocksize of the file
  BLOCKSIZE = 4096 * 1024

  # Open the file
  f = open(fname,'r')

  # Seed the checksum
  checksum = md5.md5()

  # Read until we run out of data adding to the checksum
  while True :
    data = f.read(BLOCKSIZE)
    if (not data) : break
    checksum.update(data)

  f.close()

  # Return with the computed checksum
  return checksum.hexdigest().lower()


####################################################################################################

# Send the esPayload to Elastic Search

def sendToES (type='none', tmo=15):

  # Send to ES only if enabled
  if ( not enableES ) : return ( None )

  # Build the ES Index with a /type added
  esIndex = '%s/doc' % ( esIndexBase )

  # Set the type of the record we are storing (get, put, df, rm)
  esPayload['type'] = type


  # Try to send the payload to ES

  try :
    esStatus = requests.post(esIndex, data=json.dumps(esPayload), headers={"content-type": "text/javascript"}, timeout=tmo)
    if ( str(esStatus) != '<Response [201]>' ) : warn( 58,"Failed to send to Elastic Search : %s" % esStatus)
    return ( True )

  except requests.exceptions.RequestException as e:
    warn( 59,"Error in indexing in Elastic Search: %s" % e)
    return ( False )


  # We should never get here
  return ( None )

####################################################################################################

