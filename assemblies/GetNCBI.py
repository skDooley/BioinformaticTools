from BioDatabases import NCBI
from time import sleep

from optparse import OptionParser
parser = OptionParser()
parser.add_option("-f", "--fraction",  dest="FRAC", default=1.0, help="The fraction starting point of the array")
parser.add_option("-d", "--direction", dest="DIR", action="store_false", default=True, 
	                                   help="The direction to go in the array (backwards = False, forwards = True)")

parser.add_option("-r", "--refseq",   dest="REFSEQ", action="store_true", default=False,  help="Download from RefSeq")
parser.add_option("-g", "--genbank",  dest="GB",     action="store_true",  default=False, help="Download from GenBank")
parser.add_option("-b", "--bacteria", dest="BACT",   action="store_true", default=False,  help="Download bacterial assemblies")
parser.add_option("-a", "--archaea",  dest="ARCH",   action="store_true",  default=False, help="Download archaeal assemblies")

(opts, args) = parser.parse_args()

#Download parameters
sleepTime = 3*60
downloadPath = "/mnt/research/germs/shane/databases/assemblies/NCBI"
sources = []
if opts.GB: sources.append("genbank")
if opts.REFSEQ: sources.append("refseq")

domains = []
if opts.BACT:domains.append("bacteria")
if opts.ARCH:domains.append("archaea")


#Run the downloader
attempt = 1
while True:
	try:
		downloader = NCBI(sources,domains,downloadPath, fraction=float(opts.FRAC), direction=opts.DIR)
		downloader.get_database()
	except:
		if attempt == 4: break
		print("Download attempt %i failed. Sleeping for %i minutes, then retrying." % (attempt,sleepTime))
		sleep(sleepTime)
		attempt += 1