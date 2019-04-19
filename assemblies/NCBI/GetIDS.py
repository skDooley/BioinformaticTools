import os
import pickle
from Bio.SeqIO import parse
db_path = "/mnt/research/germs/shane/databases/assemblies/NCBI/refseq"
files = os.listdir(db_path)
assembly_ids = {}
for fileName in files:
    for rec in parse(os.path.join(db_path,fileName),"fasta"): assembly_ids[rec.id]=fileName
print len(assembly_ids)
pickle.dump(assembly_ids,open("/mnt/research/germs/shane/databases/assemblies/NCBI/refseq_manifest.p","wb"))
