import urllib2
import sys
from time import time
import random

#words = ['erferf', 'dfgfg', 'ffff', 'serferf', 'ydfgfg', 'rffff'] 

def bldDict():
 word_site = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"

 response = urllib2.urlopen(word_site)
 txt = response.read()
 words = txt.splitlines()
 return words

totalWords = 0
def createDoc(dictionary):
  #create a document of a random number of words between range as given below
  global totalWords 
  docSz = random.randint(2000, 20000)
#for testing, set to a lower number of documents
  #docSz = random.randint(1, 200)
  dictSz = len(dictionary)
  print("Creating a document of " + str(docSz) + " words")
  document = ' '
  while docSz:
    word = dictionary[random.randint(0,dictSz-1)]
    #print(word)
    document += (word+ ' ')
    docSz -=1
  totalWords += docSz
  return document

try:
  docCount = sys.argv[1]
except:
  docCount = 30000

print("Creating a corpus of " + str(docCount) + " documents")
#exit(0)

def createCorpus(corpusfile, docCount):
  docId = 0
  while docCount:
    doc = createDoc(dictionary)
    corpusfile.write('nferdoccount_' + str(docId) + doc + '\n')
    corpusfile.write(delim)
    docId += 1
    docCount -= 1

delim ="nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword nferstopword \n"
if __name__== "__main__":
  dictionary = bldDict()
  corpusfile = "raw-"+ str(time()) + ".txt"
  corpusfd = open(corpusfile,"w+") 
  createCorpus(corpusfd, int(docCount))
  print("created a corpus of " + str(totalWords) + " words in " + str(docCount) + " documents")
  print("Output file: " + corpusfile)
