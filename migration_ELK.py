#! author : Inzamamul Alam
import os
from typing import Iterator, Dict, Any
from shutil import copyfile
import elasticsearch
import time
import re
from datetime import datetime, timedelta
import re
from pathlib import Path
from datetime import datetime as dt, timedelta
import json
import logging
import sched
import fileinput
import sys


#ES culster configuration--multiple/single cluster setup with authentication
es  = elasticsearch.Elasticsearch(
        ['https://52.77.45.172:9200'],
        http_auth = ('elastic','m83CzwCMR6Fyig9afYeL'),
        ca_certs= False, 
        verify_certs= False,
        ssl_show_warn= False,
        retry_on_timeout = True,
        timeout = 30
    )
#DNS track file create and get all indexs starts with dns

my_file = Path("log_temp") #needn't use
track_dns = Path("track_dns.txt") #stating dns record for resume session

if track_dns.is_file() == False:
    dns_track = open('track_dns.txt', 'a')
    for i in reversed(es.indices.get('*')):
        if i.startswith("dns") == True:
            dns_track.write("%s\n" % i)
    dns_track.close()
#track latest file for collect the last timestamp of that file for resume session
def find_latest_file(object):
    if object.is_file():
        file_track = open('track_dns.txt', 'r')
        for line in file_track:
            latest_file = line
            break
        file_track.close()
    else:
        latest_file = None
    return  latest_file

current_file = find_latest_file (track_dns)
#print(current_file)
if Path(current_file.rstrip()+'.log').is_file():
    # file exists
    with open(current_file.rstrip()+'.log', 'rb') as f:
        try:  # catch OSError in case of a one line file 
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR) #bigdata last record fetch
        except OSError:
            f.seek(0)
        last_line = f.readline().decode()
    match = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z', last_line) #regex pattern match for extract timestamp for stating timestamp record
    gte_timestamp = datetime.strptime(match.group(0), '%Y-%m-%dT%H:%M:%S.%fZ')
    print(gte_timestamp)
else:
    gte_timestamp = None
    

#individual file generate for elk migration w.r.t to index name
def write_batch(docs):
    if Path(docs[0]['index']+'.log').is_file() == False:
        with open(docs[0]['index']+'.log', 'a+') as f:
            for item in docs:
                f.write("%s resolved: %s %s\n" % (item["message"], item["domain_ip"],item["timestamp"])) #collecting and writing only message, dnslookup
                                                                                                         #and ingest timestamp
#if file is create write the others iteration
    else :
        with open(docs[0]['index']+'.log', 'a+') as f:
            for item in docs:
                f.write("%s resolved: %s %s\n" % (item["message"], item["domain_ip"],item["timestamp"]))

    
            
                

#ES query call by querying,sorting and condition based upon on resume session
def query_call(index: str = ' ') -> Iterator[Dict[str, Any]]:
    if gte_timestamp is not None:
        body = {
        'size' : 10000,
        "sort" : [
                 { "@timestamp" : {"order": "asc"}}
                 ],
        "query": {
            "bool": {
                "must": [
                   { "match_all": {} },
                    {
                      "range": {
                        "@timestamp": {
                          "format": "strict_date_optional_time",
                          "gte": gte_timestamp,
                          "lte": dt.now()
                        }
                      }
                    }
                ],
            }
        }
        }
    else:
        body = {
            'size' : 10000,
            "sort" : [
                 { "@timestamp" : {"order": "asc"}}
                 ],
            "query": { "match_all": {} }
        }
    iter = 1
    scroll = None
    while True:
        if iter == 1:
            try:
                res = es.search(index=index, body=body, scroll='1d')
                scroll = res['_scroll_id']
            except elasticsearch.NotFoundError as err:
                print(err)
                res = None
            except elasticsearch.ElasticsearchException as err:
                print(err)
                res = None
#pagination use for fetching next page with while loop
        else:
            try:
                res = es.scroll(scroll_id = scroll, scroll = '1d')
                scroll = res['_scroll_id']
 #if index is not found               
            except elasticsearch.NotFoundError as err:
                print(err)
                res = None
#if es cluster connection has problem
            except elasticsearch.ElasticsearchException as err:
                print(err)
                res = None
#if no index occur break all
        if not res:
            break

        yield res['hits']['hits']

        iter += 1


#get data and core controller for dns record save
def get_data(indices: Iterator[Dict[str, Any]])->None:
    while True:
        try:
            start = time.perf_counter()
            hits = next(indices)
            #print("Reading slot:{0}".format(len(hits)))
            elapsed = time.perf_counter() - start
            #print(f'Time: {elapsed:0.4f} seconds')

#controller for dns record save
            if not hits:
                with open("track_dns.txt", "r") as t_file:

                    for line in t_file:
                        line = line.rstrip()
                        if line == track:
                            mod_line=line.replace(line,line+" Done\n")
                        else:
                            mod_line= None

                for temp in fileinput.input("track_dns.txt", inplace=True):
                    if temp.strip().startswith(track):
                        if mod_line is not None:
                            fin_line = mod_line
                            sys.stdout.write(fin_line)
                    else:
                        sys.stdout.write(temp)
                return True

#data save into a list
            doc = []
            for hit in hits:
                data = {}
                source = hit['_source']
                index = hit['_index'] #variable for using cursor which dns index we are now
                data['timestamp'] = source['@timestamp']

                if 'domainIp' in source:
                    data['domain_ip'] = source['domainIp']
                else:
                    data['domain_ip'] = None

                if 'tags' in source:                    
                    data['tags'] = json.dumps(source['tags'])
                

                data['message'] = str(source['message'])
                data['index'] = index
                     
                doc.append(data)

                
            
            start = time.perf_counter()
            write_batch(doc)
            #print("Writing slot:{0}".format(len(doc)))
            elapsed = time.perf_counter() - start
            #print(f'Time: {elapsed:0.4f} seconds')
 
#Stop the loop when iteration is closed
        except StopIteration:
 
                break
 
    return True


def Reverse(lst):
    return [ele for ele in reversed(lst)]

#Main function
def main():
    todayIndex = "dns-{today}"
    todayTime = dt.now()
    todayIndexStr = todayIndex.format(today = todayTime.strftime('%Y.%m.%d'))
    #create a list for working in a loop as a list behavior
    with open(track_dns) as file:
        lines = file.readlines()
        lines = [line.rstrip() for line in lines]
        #lines = Reverse(lines)
        #print (lines)
        #time.sleep(10)
    #indexLists = ['dns-2020.11.20']

#Main controller
    for index in lines:
  
        global track # variable for dns index 
        track = index
        try:
            es.indices.put_settings(index=index,body= {"index" : {"max_result_window" : 100000 }})
            dataFrme = get_data(query_call(str(index)))
        except elasticsearch.NotFoundError as err:
            continue

if __name__ == "__main__":

    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    logging.info("Main    : before creating Event")
    start = time.perf_counter()
    main()
    elapsed = time.perf_counter() - start
    #print(f'Total Time spend: {elapsed:0.4f} seconds')
    logging.info("Main    : before running Event")