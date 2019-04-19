from ftplib import FTP 
ftp = FTP('ftp.ncbi.nlm.nih.gov','anonymous','wangqion@gmail.com')
cwd = "refseq/release/bacteria/" 
ftp.cwd(cwd)
files = ftp.nlst("./")
count = 0

ftpDir = "ftp://ftp.ncbi.nlm.nih.gov/" + cwd
for fname in files:
	if ".faa.gz" not in fname:continue
	ftpFile = ftpDir+fname
	ftp.retrbinary('RETR %s' % (fname) , open("refseq/"+fname,'wb').write)
	count+=1
	if count % 50 == 0: print (count)
