#!/usr/bin/env python
#

# Our site name
siteName          = 'MWT2'


# Default log file - scripts should override this
LOGFILE='/tmp/lsm.out'


#Tunable parameters
pcacheEXE        = "pcache.py"
pcacheRetries    =    3
pcacheTMO        = 1200.00
pcacheTMOfudge   =   15.00
pcacheMaxSpace   =  "80%"
#pcacheMaxSpace   =  "10T"



# Timeout values for transfers in seconds
tmoConnect       =   30.00
tmoTransfer      =  300.00
tmoMinTransfer   =   30.00

# Number of seconds to move a MB
tmoPerMB         =    0.25
tmoFAXmultiplier =    3

# Permissions
permDIR          = 0775
permFile         = 0664


# The MWT2 prefixes for the different protocols
prefixXRD        = "root://xrddoor.mwt2.org:1096"
prefixFAX        = "root://atlas-xrd-central.usatlas.org:1094"


# Local path after the prefix for SRM
SFN_ROOT         = '?SFN='

# Local path after the prefix for FAX
FAX_ROOT          = '//atlas'

# Local path after the prefix for Rucio
RUCIO_ROOT        = '/rucio'



# Use Panda Caching and the Site (Queues) to manange
pandaCache        = False
pandaSiteName     = 'MWT2_SL6,MWT2_MCORE,ANALY_MWT2_SL6'



# Elastic Search monitoring
enableES          = True

####################################################################################################
