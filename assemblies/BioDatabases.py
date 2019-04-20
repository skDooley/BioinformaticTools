import os
import sys
import datetime
import time
from ftplib import FTP 
from csv import DictReader
from pickle import dump, load
class database_source(object):
    def __init__(self,name,direction=True,fraction=1.0):
        self.name = name
        self.currentFiles = {}
        self.newFiles = {}
        self.ftp = None
        self.failed = {}
        theDate = str(datetime.date.today())
        logFileName = "%s_download_error_%s.log" % (name,theDate)
        count = 1
        while (os.path.exists(logFileName)):
            logFileName = "%s_download_error_%s.log%i" % (name,theDate,count)
            count += 1
        self.logFileName = logFileName
        self.logfh = open(logFileName,"w")
        self.dir = direction #For large databases this option allows multiple downloaders to run at once and not collide
        self.fraction = fraction #For large databases this option allows you to start at a particular percentage of the way through 
        #the list of files from that particular database.
    
    def update(self):
        try:
            self._get_new_manifest()
            self._get_current_file_state()
            self._get_new_assemblies()
            self._logFailures()
            print ("Finished downloading assemblies. Please see the log file %s for download information if there was a failure." % (self.logFileName))
            self.close() #Close FTP conection 
        except Exception as e:
            self._logFailures()
            self.close()
            raise e
    
    def log(self, msg): self.logfh.write(msg+"\n")
    
    def _logFailures(self): 
        if len(self.failed)==0:return
        for source in self.failed:
            for domain in self.failed[source]:
                if len(self.failed[source][domain]) > 0: self.log("Failed to download the following assemblies from %s %s:" % (source,domain))
                for assemName in self.failed[source][domain]: self.log("\t" + assemName)
        
        dump(self.failed,open("%s_failedAssemblies_%s.p" % (self.name,theDate), "wb"))

    def close(self):
        self.logfh.close()
        if self.ftp != None: self.ftp.close()
        if len(self.cleanupFiles)>0: os.system("rm -f %s" % (" ".join(self.cleanupFiles)))

    def ftp_exists(self,ftpFilePath,ftpDir):
        try:
            self.ftp.nlst(ftpFilePath)
            return ftpFilePath
        except:
            files = self.ftp.nlst(ftpDir)
            for file in files:
                if "_cds_" in file or "_rna_" in file: continue
                if "_genomic.fna.gz" in file: return file
            return None            

    def fastaExists(self,filePath,ftpFile):
        #If both the fasta and gz file exist:
        if os.path.exists(filePath) and os.path.exists(filePath.replace(".gz","")):
            os.system("rm %s" % (filePath)) #remove the gzipped file
            return True
        #another version of the downloader already downloaded it
        if os.path.exists(filePath.replace(".gz","")): 
            #Check that the file was fully downloaded
            try: fileSystemSize = os.stat(filePath).st_size
            except: fileSystemSize = 0
            if fileSize == 0 or fileSystemSize < self.ftp.size(ftpFile): 
                self.log("Removing", filePath, "Disk Size:",fileSize, "FTP Size:", ftp.size(ftpFile))
                os.system("rm " + filePath )
                return False
            return True 
        return False

class NCBI(database_source):
    def __init__(self, sources=['refseq'], domains=['bacteria'], downloadPath="./NCBI", direction=True, fraction=1.0):
        database_source.__init__(self,"NCBI")
        self.ftp = None
        self.cleanupFiles = []
        self.domains = domains # ['bacteria', 'archaea', 'plasmid', 'viral']
        self.sources = sources # ['genbank',  'refseq'                     ]
        self.downloadPath = downloadPath
        self.dir = 1
        if not direction: self.dir = -1
        self.fraction = fraction
        if (fraction > 1.0) or (fraction <= 0.0):
            eprint("The fraction must be between 0 and 1. Exiting run.")
            sys.exit(-1)

    def get_database(self): self.update()

    def _get_new_manifest(self):
        print("Getting newest NCBI manifests from", " and ".join(self.sources))
        for source in self.sources:
            self.newFiles[source] = {}
            for domain in self.domains:
                self.newFiles[source][domain] = {}
                theDate = str(datetime.date.today())
                manifest = "%s/%s_%s_assembly_summary_%s.txt" % (self.downloadPath , domain, source, theDate)
                cmd = "wget ftp://ftp.ncbi.nlm.nih.gov/genomes/%s/%s/assembly_summary.txt -O %s 2>/dev/null" % (source.lower(),domain,manifest)
                print("\tGetting:", manifest)
                #os.system(cmd)
                file = open(manifest,"r")
                file.readline()
                for rec in DictReader(file,delimiter="\t"): self.newFiles[source][domain][rec['# assembly_accession']] = rec
                print("\t\t%i assemblies in %s %s" % (len(self.newFiles[source][domain]),source,domain))

    def connect(self,source,domain): 
        if self.connected(): self.close() #If already connected then close the current connection to reset the connection
        self.ftp = FTP('ftp.ncbi.nlm.nih.gov','anonymous','wangqion@gmail.com')
        self.ftp.cwd("genomes/%s/%s" %(source.lower(),domain))
        self.logfh = open(self.logFileName,"a") #Reopen the error log file

    def connected(self): return self.ftp != None 

    def _get_current_file_state(self):
        #if (os.path.exist()): load(open("%s_currentAssemblies.p" % (self.name),"rb"))
        self.currentFiles = {'refseq':{'bacteria':set(),'archaea':set()},'genbank':{'bacteria':set(),'archaea':set()}}
        print ("Number of assemblies already downloaded:")
        for source in self.sources:
            for domain in self.domains:
                dirFiles = os.listdir(os.path.join(self.downloadPath,source,domain))
                for fileName in dirFiles: self.currentFiles[source][domain].add(fileName.replace(".fasta",""))
                print ("\t%i already downloaded from %s %s" % (len(self.currentFiles[source][domain]),source,domain))
  
    def _get_new_assemblies(self):
        print ("Getting undownloaded assemblies")
        for source in self.sources:
            for domain in self.domains:
                if len(self.newFiles[source][domain]) <= len(self.currentFiles[source][domain]): continue
                assembliesToGet = list(set(self.newFiles[source][domain]).difference(self.currentFiles[source][domain]))
                self.connect(source.lower(),domain)
                #TODO: Look at self.newFiles[source][domain][assembly]['seq_rel_date'] to determine if assembly needs to be replaced
                startIndex = 0
                endIndex = len(assembliesToGet)
                if(self.fraction != 1.0): startIndex = int(endIndex*self.fraction)
                if not self.dir:
                    startIndex = endIndex
                    endIndex = 0
                print("\tGetting %i assemblies from %s %s." % (abs(endIndex-startIndex),source,domain))
                count = 0
                for assembly in assembliesToGet[startIndex:endIndex:self.dir]:
                    filePath = "%s/%s/%s/%s.fasta.gz" %(self.downloadPath,source,domain,assembly)
                    ftpDir = self.newFiles[source][domain][assembly]['ftp_path'].replace("ftp://ftp.ncbi.nlm.nih.gov","")
                    ftpFile = "%s/%s_%s_genomic.fna.gz" % (ftpDir,assembly,self.newFiles[source][domain][assembly]['asm_name'].replace(" ","_").replace("#","_"))
                    try:
                        if not self.fastaExists(filePath,ftpFile):
                            ftpFile = self.ftp_exists(ftpFile,ftpDir)
                            if not ftpFile: raise(Exception("File is not accessable or the ftp failed"))
                            self.ftp.retrbinary('RETR %s' % (ftpFile) , open(filePath,'wb').write)
                            os.system("gunzip -q -f -d %s" % filePath)
                            count += 1
                            if ((count % 1000) == 0): print("\t\tCompleted %i assemblies of %i" % (count,len(assembliesToGet[startIndex:endIndex])))
                        self.currentFiles[source][domain] = assembly #Add the assembly to assemblies completed
                    except Exception as e:
                        os.system("rm -f "+filePath)
                        self.close()
                        time.sleep(3) # sleep 3 seconds after a failure
                        self.connect(source,domain)
                        ftpFile = self.ftp_exists(ftpFile,ftpDir)
                        
                        if ftpFile != None:
                            self.ftp.retrbinary('RETR %s' % (ftpFile) , open(filePath,'wb').write)
                            os.system("gunzip -q -f -d %s" % filePath)
                            count+=1
                        else:
                            badFile = assembly+" "+ftpDir
                            try:self.failures[source][domain].append(badFile)
                            except:
                                try: self.failures[source][domain] = [badFile]
                                except: self.failures[source] = {domain:[badFile]}
