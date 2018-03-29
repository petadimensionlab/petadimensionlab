SEARCH_WORD = '"gout" or"gouty"'

THEME = 'DID034'

WS_DIR = '/Users/petadimensionlab/ws/refdb/bacteria'

# -*- coding: utf-8 -*-
import re, os, sys, glob, operator, shutil, tarfile, sqlite3
from datetime import datetime 
from dateutil import parser
from xml.etree.ElementTree import ElementTree, parse
from Bio import Entrez
Entrez.email = 'petadimension@yahoo.co.jp'

THEME_DIR = os.path.join(WS_DIR,THEME)
if not os.path.isdir(THEME_DIR):
    ndir = os.getcwd()
    os.chdir(WS_DIR)
    os.mkdir(THEME)
    os.chdir(ndir)

now = datetime.today()
present_time = now.strftime('%Y-%m-%d')

class Pubmed:
    def __init__(self,THEME):
        self.header = THEME
        self.db_name = self.header+'Ref.db'
        self.type_lst = ['abstract', 'author', 'journal', 'journal-full', 'MeSH', 'pub-type', 'ref', 'title', 'url', 'year']
        self.freq_lst = ['author', 'journal', 'MeSH', 'year']
        self.date = present_time
        #print 'Data collection available for ', ' '.join(self.type_lst)
        #print 'Calculation of frequency available for ', ' '.join(self.freq_lst)
    def reset_dir(self):
        if not os.path.isdir(os.path.join(os.path.join(WS_DIR, self.header),'stat')):
            ndir = os.getcwd()
            os.chdir(os.path.join(WS_DIR, self.header))
            os.mkdir('stat')
            os.chdir(ndir)
        STAT_DATA_DIR = os.path.join(os.path.join(WS_DIR, self.header),'stat')
        XML_DIR = os.path.join(os.path.join(WS_DIR, self.header),'pubmed_xml')
        return STAT_DATA_DIR, XML_DIR
    def first_fetch(self,query='Psoriasis vulgaris'):
        #if not os.path.isdir(XML_DIR):
        #    os.mkdir(XML_DIR)
        os.chdir(THEME_DIR)
        #STAT_DATA_DIR, XML_DIR = self.reset_dir()
        search_results = Entrez.read(Entrez.esearch(db="pubmed", term=query, retmax=100000, usehistory="y"))
        count = int(search_results["Count"])
        if count>100000:
            search_results = Entrez.read(Entrez.esearch(db="pubmed", term=query, retmax=count, usehistory="y"))
            count = int(search_results["Count"])
        print("Found %i results" % count)
        batch_size = 200
        tmp_xml = THEME+'.xml'
        out_handle = open(tmp_xml, 'w')
        for start in range(0, count, batch_size):
            end = min(count, start+batch_size)
            print("In Processing... %i to %i" % (start+1, end))
            fetch_handle = Entrez.efetch(db="pubmed", rettype="medline", retmode="xml", retstart=start, retmax=batch_size, webenv=search_results["WebEnv"], query_key=search_results["QueryKey"])
            data = fetch_handle.read() ##, encoding='utf-8'##
            fetch_handle.close()
            try:
                out_handle.write(data)
            except:
                print('%d :Failed in Writing PubMed information in a xml-file.' % start)
        out_handle.close()
        ## separate a single XML file ##
        REPstart = re.compile('<PubmedArticle>')
        REPend = re.compile('</PubmedArticle>')
        REPpmid = re.compile('\d{2,}')
        fr = open(tmp_xml).readlines()
        start_lst = []
        end_lst = []
        total = len(fr)
        for i in range(0,total):
            line = fr[i]
            if REPstart.match(line):
                start_lst.append(str(i))
            elif REPend.match(line):
                end_lst.append(str(i))
        pmid_lst = []
        pmid_err = []
        for loc in start_lst:
            line = fr[int(loc)+2]
            res = REPpmid.search(line)
            if res=='None':
                pmid_err.append(loc)
            else:
                pmid = res.group(0)
                pmid_lst.append(str(pmid))
        doc_num = len(pmid_lst)
        for i in range(0,doc_num):
            save_file = pmid_lst[i]+'.xml'
            fw = open(save_file, 'w')
            fw.write(fr[0])
            fw.write(fr[1])
            fw.write(fr[2])
            for j in range(int(start_lst[i]), int(end_lst[i])+1):
                fw.write(fr[j])
            fw.write('</PubmedArticleSet>')
            fw.close()
        err_file = THEME+'_'+'err'+present_time+'.txt'
        fw = open(err_file,'w')
        for item in pmid_err:
            fw.write(str(item)+'\n')
        fw.close()
        os.remove(os.path.join(THEME_DIR,tmp_xml)) # remove non-separated xml file
        ## list of separated xml files ##
        sep_xml = glob.glob('*.xml')
        pmid_list_file = THEME+'_'+'PMID_'+present_time+'.txt'
        fw = open(pmid_list_file, 'w')
        for item in sep_xml:
            ID, ext = item.split('.')
            fw.write(ID+'\n')
        fw.close()
        os.chdir(WS_DIR)
        cmd = 'tar -cjf %s %s' % (THEME+'_'+present_time+'.tar.bz2',THEME+'/')
        os.system(cmd)
        ## delete files ##
        cmd = 'rm -rf %s' % (THEME_DIR)
        os.system(cmd)
        msg = 'Collection completed: %s' % (THEME)
        print(msg)
        #os.chdir(THEME_DIR)
        #for item in sep_xml:
        #    os.remove(item)

if __name__=='__main__':
    pm = Pubmed(THEME)
    pm.first_fetch(SEARCH_WORD)