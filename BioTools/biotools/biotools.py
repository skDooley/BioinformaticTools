'''
Tools written for the express intent of making the author's life easier
'''

from Bio.Blast.Applications import NcbiblastnCommandline
from Bio.Blast import NCBIXML
def createBLASTdb(refFileName,outputDBName="data/ref"):
    os.system("makeblastdb -in %s -dbtype \"nucl\" -out %s > NUL" %(refFileName,outputDBName))
    return outputDBName

def BLAST(seqs,fragments,eVal=0.001,word_size=11):
    queryFile, outputDBname = prepareSequences(seqs,fragments)
    NcbiblastnCommandline(cmd='blastn', out='BlastOut.xml', outfmt=5, strand='both', query=queryFile, db=outputDBname, evalue=eVal, word_size=word_size)()
    results = NCBIXML.parse(open('BlastOut.xml','r'))
    return results

#How to parse results:
#    for result in results:
#        for alignment in result.alignments:
#            for hsp in alignment.hsps: 