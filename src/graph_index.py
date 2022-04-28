import datetime
import sys
import requests
import pickle
import hashlib
from optparse import OptionParser

def graph_index_iterator(end_point, size, last=0, end=sys.maxsize):
    headers = {"Content-Type": "application/sparql-query",
                             "Accept":"application/json"}
    query_string = """
    select ?t ?p ?o ?s {
        ?s a ?t .
        ?s ?p ?o .
    } limit %s
    offset %s
    """ % (size, last+size)
    res = requests.post(end_point,
                       headers=headers,
                       data=query_string)
    if res.status_code != 200:
        return
    result = res.json()["results"]["bindings"]
#    print(result, last, end)
    while len(result) > 0 and end > last:
        print("next block starting at", last, datetime.datetime.now())
        for r in result:
            yield r
        last += len(result)
#        print(result, last, end)
        query_string = """
            select ?t ?p ?o ?s {
                ?s a ?t .
                ?s ?p ?o .
            } limit %s
            offset %s
            """ % (size, last+size)
        res = requests.post(end_point,
                           headers=headers,
                           data=query_string)
        if res.status_code != 200:
            return
        result = res.json()["results"]["bindings"]

class GraphIndex():
    def __init__(self):
        self.ndx = {}
        self.reverse_uri = {}
        
    def hash_uri(self, uri):
        # maintain reverse_uri as a side_effect
        hash_value = int.from_bytes(hashlib.sha256(uri.encode("utf-8")).digest(), byteorder="big")
        if self.reverse_uri.get(hash_value) == None:
            self.reverse_uri[hash_value] = uri
        return hash_value
    
    def add(self, typed_statement):
        t = self.hash_uri(typed_statement["t"]["value"])
        p = self.hash_uri(typed_statement["p"]["value"])
        o = self.hash_uri(typed_statement["o"]["value"]) if typed_statement["o"]["type"] == "uri" else typed_statement["o"]["value"]
        s = self.hash_uri(typed_statement['s']["value"])
    #    print(t, p, o, s)
        if self.ndx.get(t) == None:
            self.ndx[t] = {}
        if self.ndx[t].get(p) == None:
            self.ndx[t][p] = {}
        if self.ndx[t][p].get(o) == None:
            self.ndx[t][p][o] =set()
        self.ndx[t][p][o].add(s)
        
    def query(self, subject_type, predicate, object_value):
        print(type(object_value))
        if callable(object_value):
            try:
                res = {}
                for k, v in self.ndx[self.hash_uri(subject_type)][self.hash_uri(predicate)].items():
#                    print(subject_type, predicate, k, v)
                    key = object_value(k)
                    if res.get(key) == None:
                        res[key] = set()
                    res[key] = res[key].union(v)
                return res
            except Exception as e:
                print(e)
                return {}
        elif object_value != None:
            print("Some here")
            try:
                return {self.reverse_uri(x) for x in self.ndx[self.uri(subject_type)][self.uri(predicate)][object_value]}
            except:
                return set()
        else: # only type and predicate given
            try:
                res = {}
                for k, v in self.ndx[self.hash_uri(subject_type)][self.hash_uri(predicate)].items():
                    k = self.reverse_uri[k] if isinstance(k, int) else k
                    res[k] =  v
                return res
            except Exception as e:
                print(e)
                return {}
        
def main():
    index = GraphIndex()
    for r in graph_index_iterator(options.endpoint, 50000, 0, options.max_records):# , 200000): #, 2500000):
        index.add(r)
    
    with open(options.output_file, "wb") as f:
        pickle.dump(index, f)
    print("Done")

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-e", "--endpoint", dest="endpoint",
                    help="sparql endpoint", metavar="ENDPOINT"
                    )
    parser.add_option("-m", "--max", dest="max_records",
                    help="max records to process", metavar="MAX_RECORDS",
                    default=sys.maxsize, type="int",
                    )
    parser.add_option("-o", "--output", dest="output_file",
                    help="output file", metavar="OUTPUT"
                    , default="/usr/share/index.pickle")
    (options, args) = parser.parse_args()
    print(options, args)  
    main()