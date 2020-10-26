from datetime import date
import glob
import sys
from os import chdir, listdir, path, system
import pickle
#Paths
base = "/mnt/research/germs/shane/databases/crisprs"
dba_base = "/mnt/research/germs/shane/databases/assemblies/"
refDatabases = {"Corteva":".fasta","NCBI/refseq/archaea":".fasta","NCBI/refseq/bacteria":".fasta","NCBI/genbank/bacteria":'.fasta',"NCBI/genbank/archaea":'.fasta',"PATRIC2/fastas":".fna"}

#Remove old hpc scrips
system("rm -f " +base+"/scripts/hpc_scripts/*")

#Get Todays date for the scripts
theDate = str(date.today())

#HPC Header
header = ""
for line in open("header.sb"): header+=line

firstFileName = base+"/scripts/hpc_scripts/CRISPR_run_%i_%s.sb" % (0,theDate)
bashLog = open(firstFileName, 'w')
bashLog.write(header)
logNumber,count =0,0
validExts = set([".fna",".fasta"])

pCompletedAssemblies = pickle.load(open("PilerCR_Files.p","rb"))
mCompletedAssemblies = pickle.load(open("MinCED_Files.p","rb"))

for assembly_dir, ext in refDatabases.items():
    bashLog.write("cd "+ dba_base + assembly_dir+"\n")
    # print("Cleaning up")
    # system("rm -f %s/core* %s/*.tmp %s/*.gz" % (dba_base+assembly_dir,dba_base+assembly_dir,dba_base+assembly_dir))
    print("Getting assemblies from " + assembly_dir)
    assemblies = listdir(dba_base+assembly_dir)
    print("\t%i assemblies in %s" % (len(assemblies),assembly_dir))

    pilerOut = base + "/pilerCR/pat2"; mincedOut = base + "/minCED/pat2"
    if "refseq" in assembly_dir:  pilerOut = base + "/pilerCR/refseq"; mincedOut = base + "/minCED/refseq"
    if "genbank" in assembly_dir: pilerOut = base + "/pilerCR/genbank"; mincedOut = base + "/minCED/genbank"
    asmCount = 0
    for assembly in assemblies:
        if assembly[assembly.rfind("."):] not in validExts: continue
        #Add PilerCR command
        pcrPath = "%s/%s.pcrout" % (pilerOut, assembly.replace("%s"%(ext),""))
        if not path.exists(pcrPath): #pcrPath not in pCompletedAssemblies: # 
            asmCount += 1
            pcmd = "pilercr -minid 0.85 -mincons 0.8 -noinfo -in %s -out %s/%s.pcrout 2>/dev/null" %(assembly, pilerOut, assembly.replace("%s" %(ext),""))
            count+=1
            bashLog.write(pcmd+"\n")
            if (count % 100 == 0):
                logNumber+=1
                bashLog.write("rm -f core* *.tmp *.gz\n")
                bashLog.close()
                bashLog = open(base+"/scripts/hpc_scripts/CRISPR_run_%i_%s.sb" % (logNumber,theDate), 'w')
                bashLog.write(header)
                bashLog.write("cd "+ dba_base + assembly_dir + "\n")
        pCompletedAssemblies.add(pcrPath)

        #Add minCED command
        minCDPath = "%s/%s.mnout" % (mincedOut, assembly.replace("%s" %(ext),""))
        if not path.exists(minCDPath): # minCDPath not in mCompletedAssemblies: #
            asmCount += 1
            mcmd = "minced -minRL 16 -minSL 8 -maxRL 64 -maxSL 64 -searchWL 6 %s %s/%s.mnout" %(assembly, mincedOut, assembly.replace("%s" %(ext),""))
            count+=1
            bashLog.write(mcmd+"\n")
            if (count % 100 == 0):
                logNumber+=1
                bashLog.write("rm -f core* *.tmp *.gz\n")
                bashLog.close()
                bashLog = open(base+"/scripts/hpc_scripts/CRISPR_run_%i_%s.sb" % (logNumber,theDate), 'w')
                bashLog.write(header)
                bashLog.write("cd "+ dba_base + assembly_dir + "\n")
        mCompletedAssemblies.add(minCDPath)
    print("\t\t%i assemblies from %s" % (asmCount,assembly_dir))

bashLog.close() 

pickle.dump(pCompletedAssemblies,open("PilerCR_Files.p","wb"))
pickle.dump(mCompletedAssemblies,open("MinCED_Files.p","wb"))

print("\nThere are %i logs for %i assemblies" % (logNumber,count))


        #retCode1 = system(pcmd)
        
    # #Run minced
    # if path.exists("%s/%s.mnout" % (outdir, assembly)):retCode2 = 0
    # else: 
    #     mcmd = "minced -minRL 16 -maxRL 64 -minSL 8 -maxSL 64 -searchWL 6 %s %s/%s.mnout" % (assembly_dir+assembly, outdir, assembly)
    #     # print cmd
    #     retCode2 = system(mcmd)
    
    # if retCode1 != 0: 
    #     error_log.write(str(retCode1) + "  " + pcmd+"\n")
    #     break
    # if retCode2 != 0: 
    #     print "%i\nminced -maxSL 75 --maxRL 75 -minRL 16 -minSL 20 -searchWL 6 %s %s/%s.mnout" % (retCode2,assembly_dir+assembly, outdir, assembly)
    #     break
