"""
@author Wenting Zheng
@author Nathaniel Brooks

Tests the features found in the index.py file.
"""


import unittest
import os
import time
import shutil
import tarfile
import stat
import gzip
import subprocess
import threading
import socket
import elasticsearch
import signal
import http.server
import json
import ndjson

import index
import fields


CODE_SRC_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(CODE_SRC_DIR, "test-data", "Index")


class IndexDataTestCase(unittest.TestCase):
    """ Tests the indexing functionality """
    
    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.makedirs(self.tmp_dir)
        self.assertTrue(os.path.isdir(self.tmp_dir))
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        self.assertTrue(not os.path.exists(self.tmp_dir))
    
    def test_set_data(self):
        aaa_file = os.path.join(self.tmp_dir, "aaa.txt")
        with open(aaa_file, "w",newline="\n") as fd:
            fd.write("xyz\n");
            fd.write("pqr\n");
            fd.write("abc\n");
        self.assertTrue(os.path.exists(aaa_file))
        self.assertTrue(os.path.isfile(aaa_file))
        
        docs = [
            {
                "_source": {
                    "case":"4007",
                    "node_name":fields.MISSING_NODE_NAME,
                    "major_version":fields.MISSING_SG_VER[0],
                    "minor_version":fields.MISSING_SG_VER[1],
                    "platform":fields.MISSING_PLATFORM,
                    "categorize_time":1957,
                    "message":"xyz\n",
                },
            },
            {
                "_source": {
                    "case":"4007",
                    "node_name":fields.MISSING_NODE_NAME,
                    "major_version":fields.MISSING_SG_VER[0],
                    "minor_version":fields.MISSING_SG_VER[1],
                    "platform":fields.MISSING_PLATFORM,
                    "categorize_time":1957,
                    "message":"pqr\n",
                },
            },
            {
                "_source": {
                    "case":"4007",
                    "node_name":fields.MISSING_NODE_NAME,
                    "major_version":fields.MISSING_SG_VER[0],
                    "minor_version":fields.MISSING_SG_VER[1],
                    "platform":fields.MISSING_PLATFORM,
                    "categorize_time":1957,
                    "message":"abc\n",
                },
            },
        ]
        
        nodefields = fields.NodeFields(case_num="4007")
        for expected,actual in zip(docs, index.set_data(aaa_file, 1957, nodefields)):
            self.assertEqual(expected, actual)
        
        return
    
    def test_send_to_es(self):
        
        def mock_elasticsearch_server():
            with socket.socket() as list_sock:
                list_sock.settimeout(10.0)
                list_sock.bind(("localhost", 48982))
                list_sock.listen(1)
                sock,addr = list_sock.accept()
            
            with sock as s:
                s.settimeout(10.0)
                
                m = ""
                while not m.endswith("}\n"):
                    m += s.recv(2048).decode()      # 2048 bytes is enough for testing
                    #print(" ---- RECEIVED:", m[-100:].encode(), flush=True)
                    
                body = m[m.index("\r\n\r\n")+4:]
                #print(" ------ HEADER:", m[:m.index("\r\n\r\n")].encode(), "\n", flush=True)
                #print(" ------ BODY:", body.encode(), "\n", flush=True)
                
                for expec, real in zip(docs, (obj for obj in ndjson.loads(body) if "index" not in obj)):
                    self.assertTrue("categorize_time" in real)
                    self.assertTrue("categorize_time" in expec)
                    del real["categorize_time"]     # index time is unknown, no compare
                    del expec["categorize_time"]
                    self.assertEqual(expec, real)
                    
                json_str = ndjson.dumps([{"items" : [] }])
                payload = 'HTTP/1.1 200 OK\r\nContent-Length: %d\r\nContent-Type: application/json\r\n\r\n%s' % (len(json_str), json_str)
                s.sendall(payload.encode())
                #print(" ----- SENT:", payload.encode(),"\n",flush=True)
                
                while s.recv(2048):
                    s.sendall("{}".encode())
                    #data = s.recv(1024).decode()
                    #if not data: break
                    #print(" ---- RECEIVED:", data,flush=True)
                    #s.sendall("Good".encode())
        
        es_thread = threading.Thread(target=mock_elasticsearch_server)
        es_thread.start()
        
        aaa_file = os.path.join(self.tmp_dir, "aaa.txt")
        with open(aaa_file, "w",newline="\n") as fd:
            fd.write("xyz\n");
            fd.write("pqr\n");
            fd.write("abc\n");
        self.assertTrue(os.path.exists(aaa_file))
        self.assertTrue(os.path.isfile(aaa_file))
        
        docs = [
            {
                "case":"4007",
                "node_name":fields.MISSING_NODE_NAME,
                "major_version":fields.MISSING_SG_VER[0],
                "minor_version":fields.MISSING_SG_VER[1],
                "platform":fields.MISSING_PLATFORM,
                "categorize_time":1957,
                "message":"xyz\n",
            },
            {
                "case":"4007",
                "node_name":fields.MISSING_NODE_NAME,
                "major_version":fields.MISSING_SG_VER[0],
                "minor_version":fields.MISSING_SG_VER[1],
                "platform":fields.MISSING_PLATFORM,
                "categorize_time":1957,
                "message":"pqr\n",
            },
            {
                "case":"4007",
                "node_name":fields.MISSING_NODE_NAME,
                "major_version":fields.MISSING_SG_VER[0],
                "minor_version":fields.MISSING_SG_VER[1],
                "platform":fields.MISSING_PLATFORM,
                "categorize_time":1957,
                "message":"abc\n",
            },
        ]
        
        #with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        #    sock.connect(("localhost", 48982))
        #    sock.sendall(b"Hello, network!")
        
        es_obj = elasticsearch.Elasticsearch([{'host':'localhost','port':48982}], verify_certs=False, timeout=20)
        nodefields = fields.NodeFields(case_num="4007")
        
        index.send_to_es(es_obj, nodefields, aaa_file)
        #print("All sent", flush=True)
        
        es_obj.transport.close()
        
        es_thread.join()


if __name__ == '__main__':
    unittest.main()

