import os
import sys
import datetime
import time
from ftplib import FTP 
from csv import DictReader
from pickle import dump, load
class database_source(object):
    def __init__(self,name):
        self.name = name
        self.currentFiles = {}
        self.newFiles = {}
        self.ftp = None
        self.failed = {}
    def update(self):
        self._get_new_manifest()
        self._get_current_file_state()
        self._get_new_assemblies()
        try: print "Finished update"
        except: print "In Error"
        finally: 
            print "Got the assembly now time to pickle"
            dump(self.currentFiles,open("%s_currentAssemblies.p" % (self.name),"wb"))
            theDate = str(datetime.date.today())
            if len(self.failed)>0: 
                for val in self.failed:
                    print ("Failed:",val)
                dump(self.failed,open("%s_failedAssemblies_%s.p" % (self.name,theDate), "wb"))
            #TODO notify user is assemblies failed
            self.close() #Close FTP conection
        if len(self.failed)>0:
            print ("Failed:",len(self.failed))
            dump(self.failed,open("%s_failedAssemblies_%s.p" % (self.name,theDate), "wb"))         
    def close(self):
        if self.ftp != None: self.ftp.close()
        if len(self.cleanupFiles)>0: os.system("rm -f %s" % (" ".join(self.cleanupFiles)))

def ftp_exists(ftp,ftpFilePath,ftpDir):
    try:
        ftp.nlst(ftpFilePath)
        return ftpFilePath
    except:
        files = ftp.nlst(ftpDir)
        for file in files:
            if "_cds_" in file or "_rna_" in file:continue
            if "_genomic.fna.gz" in file: return file
        return None            

def checkFilePath(ftp,filePath,ftpFile):
    #If both the fasta and gz file exist:
    #print "Looking for:",ftpFile
    if os.path.exists(filePath) and os.path.exists(filePath.replace(".gz","")):
        print ("Removing",filePath,filePath.replace(".gz",""))
        os.system("rm %s %s" % (filePath,filePath.replace(".gz","") ))
        return True
    #If only 1 of the files exist
    if os.path.exists(filePath) or os.path.exists(filePath.replace(".gz","")): #check the the fasta or the fasta.gz exists
        #GZ file that exists
        if os.path.exists(filePath):
            try: fileSize = os.stat(filePath).st_size
            except:fileSize = 0
            if fileSize == 0 or fileSize < ftp.size(ftpFile): 
                print ("Removing", filePath, "Disk Size:",fileSize, "FTP Size:", ftp.size(ftpFile))
                os.system("rm " + filePath ) #If GZ is zero or less than the size on the ftp
            else: 
                print("These should be the same",ftp.size(ftpFile), fileStats.st_size,filePath, ftp.size(ftpFile)==fileStats.st_size)
                sys.exit(0)
                return ftp.size(ftpFile)==fileStats.st_size
            return True
        #Fasta exists
        return False
    return True


    
class NCBI(database_source):
    def __init__(self,sources=set(['genbank','refseq']),downloadPath="./"):
        database_source.__init__(self,"NCBI")
        self.ftp = None #FTP('ftp.ncbi.nlm.nih.gov','anonymous','wangqion@gmail.com')
        self.cleanupFiles = []
        self.domains = set(['archaea']) #,'plasmid','viral'])
        self.sources = sources
        self.downloadPath = downloadPath
    def get_database(self):
        self._get_new_manifest()
        self._get_current_file_state()
        self._get_new_assemblies()
    def _get_new_manifest(self):
        print "Getting newest NCBI manifests"
        for source in self.sources:
            self.newFiles[source] = {}
            for domain in self.domains:
                self.newFiles[source][domain] = {}
                theDate = str(datetime.date.today())
                manifest = "%s/%s_%s_assembly_summary_%s.txt" % (self.downloadPath , domain, source, theDate)
                cmd = "wget ftp://ftp.ncbi.nlm.nih.gov/genomes/%s/%s/assembly_summary.txt -O %s" % (source.lower(),domain,manifest)
                os.system(cmd)
                print ("\t",cmd)
                file = open(manifest,"r")
                file.readline()
                for rec in DictReader(file,delimiter="\t"): self.newFiles[source][domain][rec['# assembly_accession']] = rec
    def connect(self,source,domain): 
        if self.connected(): self.close()
        self.ftp = FTP('ftp.ncbi.nlm.nih.gov','anonymous','wangqion@gmail.com')
        cwd = "genomes/%s/%s" %(source.lower(),domain)
        self.ftp.cwd(cwd)

    def connected(self):return self.ftp != None 
    def _get_current_file_state(self):
        self.currentFiles = {'refseq':{'bacteria':set(),'archaea':set()},'genbank':{'bacteria':set(),'archaea':set()}} #{} #load(open("%s_currentAssemblies.p" % (self.name),"rb"))
        print ("Number of assemblies all ready downloaded:")
        for source in self.sources:
            for domain in self.domains: 
                for fileName in os.listdir("/mnt/research/germs/shane/databases/assemblies/NCBI/%s/%s" % (source,domain)):
                    self.currentFiles[source][domain].add(fileName.replace(".fasta",""))
                print ("\t%s in %s: %i" % (domain,source,len(self.currentFiles[source][domain])))
  
    def _get_new_assemblies(self):
        print ("Getting new assemblies")
        for source in self.sources:
            for domain in self.domains:
                if len(self.newFiles[source][domain]) > len(self.currentFiles[source][domain]): 
                    assembliesToGet = list(set(self.newFiles[source][domain]).difference(self.currentFiles[source][domain]))
                    
                    print ("Number to get:", len(assembliesToGet), len(self.currentFiles[source][domain]), len(self.newFiles[source][domain]))
                    self.connect(source.lower(),domain)
                    #TODO In the future need to look at self.newFiles[source][domain][assembly]['seq_rel_date'] to determine if assembly needs to be replaced
                    halfway = int(len(assembliesToGet)/2.0)
                    quarterway = halfway/2
                    count = len(assembliesToGet)
                    totalLen = len(assembliesToGet)
                    print (source,domain, "Assemblies to download:",totalLen,"/", len(self.newFiles[source][domain]))
                    for assembly in assembliesToGet[::-1]: #[quarterway+halfway::-1]:
                        try:
                            filePath = "NCBI/%s/%s/%s.fasta.gz" %(source,domain,assembly)
                            ftpDir = self.newFiles[source][domain][assembly]['ftp_path'].replace("ftp://ftp.ncbi.nlm.nih.gov","")
                            ftpFile = "%s/%s_%s_genomic.fna.gz" % (ftpDir,assembly,self.newFiles[source][domain][assembly]['asm_name'].replace(" ","_").replace("#","_"))
                            if checkFilePath(self.ftp,filePath,ftpFile):
                                ftpFile = ftp_exists(self.ftp,ftpFile,ftpDir)
                                self.ftp.retrbinary('RETR %s' % (ftpFile) , open(filePath,'wb').write)
                                os.system("gunzip -q -f -d %s" % filePath)
#                                 if count % 3 == 0: time.sleep(1)
                            self.currentFiles[source][domain][assembly] = self.newFiles[source][domain][assembly] #Add the assembly to assemblies completed
#                             count -= 1
#                             if count % 1000 == 0:print "Completed downloading",totalLen-count, "of", totalLen
                        except Exception, e:
                            os.system("rm -f "+filePath)
                            self.close()
                            time.sleep(10)
                            self.connect(source,domain)
                            ftpFile = ftp_exists(self.ftp,ftpFile,ftpDir)
                            print ("Failed",source,domain,assembly,ftpFile)
                            if ftpFile != None:
                                self.ftp.retrbinary('RETR %s' % (ftpFile) , open(filePath,'wb').write)
                                os.system("gunzip -q -f -d %s" % filePath)
                                #print "Completed",source,domain,assembly
                                count -= 1
