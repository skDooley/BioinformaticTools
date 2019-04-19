from BioDatabases import NCBI
import time
import os
mulipleTries = 0
while True:
	try:
		downloadPath = "NCBI/genbank"
		getData = NCBI(["genbank"],downloadPath)
		getData.get_database()
	except:
		time.sleep(3*60)
		files = os.listdir("/mnt/research/germs/shane/databases/assemblies/NCBI/refseq/bacteria")
		mulipleTries += 1
		if mulipleTries >= 4: break
