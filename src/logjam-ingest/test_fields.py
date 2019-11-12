"""
@author Wenting Zheng
@author Nathaniel Brooks

Tests the features found in the fields.py file.
"""


import unittest
import os
import time
import shutil
import tarfile
import stat
import gzip
import subprocess

import fields


CODE_SRC_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(CODE_SRC_DIR, "test-data", "Fields")


class NodeFieldsTestCase(unittest.TestCase):
    """ Test case for the NodeFields object """
    
    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.makedirs(self.tmp_dir)
        self.assertTrue(os.path.isdir(self.tmp_dir))
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        self.assertTrue(not os.path.exists(self.tmp_dir))
    
    def test_from_lumberjack_only_verion(self):
        lumber_dir = os.path.join(self.tmp_dir, "gridid_542839", "nodename_london", "2015-2017")
        os.makedirs(lumber_dir)
        self.assertTrue(os.path.isdir(lumber_dir))
        
        lumber_file = os.path.join(lumber_dir, "lumberjack.log")
        with open(lumber_file, "w") as fd:
            fd.write("lumberjack.log file!"+"\n")
        self.assertTrue(os.path.isfile(lumber_file))
            
        sys_file = os.path.join(lumber_dir, "system_commands")
        with open(sys_file, "w") as fd:
            fd.write("storage-grid-release-100.100.100-12345678.0224.asdfg12345"+"\n")
            fd.write("random garbage text")
        self.assertTrue(os.path.isfile(sys_file))
        
        f = fields.NodeFields.from_lumberjack_dir(lumber_dir)
        self.assertEqual(fields.MISSING_CASE_NUM, f.case_num)
        self.assertEqual("100.100.100-12345678.0224.asdfg12345", f.sg_ver)
        self.assertEqual(fields.MISSING_PLATFORM, f.platform)
        self.assertEqual(fields.MISSING_CATEGORY, f.category)
        self.assertEqual("2015-2017", f.time_span)
        self.assertEqual("nodename_london", f.node_name)
        self.assertEqual("gridid_542839", f.grid_id)
    
    def test_init_none(self):
        f = fields.NodeFields()
        self.assertEqual(fields.MISSING_CASE_NUM, f.case_num)
        self.assertEqual(fields.MISSING_SG_VER, f.sg_ver)
        self.assertEqual(fields.MISSING_PLATFORM, f.platform)
        self.assertEqual(fields.MISSING_CATEGORY, f.category)
        self.assertEqual(fields.MISSING_TIME_SPAN, f.time_span)
        self.assertEqual(fields.MISSING_NODE_NAME, f.node_name)
        self.assertEqual(fields.MISSING_GRID_ID, f.grid_id)
    
    def test_init_partial(self):
        f = fields.NodeFields(sg_ver="2.3.2-85qvk", category="bycast")
        self.assertEqual(fields.MISSING_CASE_NUM, f.case_num)
        self.assertEqual("2.3.2-85qvk", f.sg_ver)
        self.assertEqual(fields.MISSING_PLATFORM, f.platform)
        self.assertEqual("bycast", f.category)
        self.assertEqual(fields.MISSING_TIME_SPAN, f.time_span)
        self.assertEqual(fields.MISSING_NODE_NAME, f.node_name)
        self.assertEqual(fields.MISSING_GRID_ID, f.grid_id)

    def test_init_all(self):
        f = fields.NodeFields(  case_num="2001293881", sg_ver="2.3.2",
                                platform="Sandhawk", category="bycast",
                                time_span="2017-2018", node_name="london",
                                grid_id="97683")
        self.assertEqual("2001293881", f.case_num)
        self.assertEqual("2.3.2", f.sg_ver)
        self.assertEqual("Sandhawk", f.platform)
        self.assertEqual("bycast", f.category)
        self.assertEqual("2017-2018", f.time_span)
        self.assertEqual("london", f.node_name)
        self.assertEqual("97683", f.grid_id)
    
    def test_inherit_missing_none(self):
        old_f = fields.NodeFields(  case_num="2001293881", sg_ver="2.3.2",
                                    platform="Sandhawk", category="bycast",
                                    time_span="2017-2018", node_name="london",
                                    grid_id="97683")
        
        new_f = fields.NodeFields(  case_num="2001293900", sg_ver="2.5.1",
                                    platform="Titan", category="system",
                                    time_span="2015-2016", node_name="paris",
                                    grid_id="76544")
        self.assertEqual("2001293900", new_f.case_num)
        self.assertEqual("2.5.1", new_f.sg_ver)
        self.assertEqual("Titan", new_f.platform)
        self.assertEqual("system", new_f.category)
        self.assertEqual("2015-2016", new_f.time_span)
        self.assertEqual("paris", new_f.node_name)
        self.assertEqual("76544", new_f.grid_id)
        
        new_f.inherit_missing_from(old_f)
        self.assertEqual("2001293900", new_f.case_num)
        self.assertEqual("2.5.1", new_f.sg_ver)
        self.assertEqual("Titan", new_f.platform)
        self.assertEqual("system", new_f.category)
        self.assertEqual("2015-2016", new_f.time_span)
        self.assertEqual("paris", new_f.node_name)
        self.assertEqual("76544", new_f.grid_id)
    
    def test_inherit_missing_partial(self):
        old_f = fields.NodeFields(  case_num="2001293881", sg_ver="2.3.2",
                                    platform="Sandhawk", category="bycast",
                                    time_span="2017-2018", node_name="london",
                                    grid_id="97683")
        
        new_f = fields.NodeFields(sg_ver="2.5.1", category="system")
        self.assertEqual(fields.MISSING_CASE_NUM, new_f.case_num)
        self.assertEqual("2.5.1", new_f.sg_ver)
        self.assertEqual(fields.MISSING_PLATFORM, new_f.platform)
        self.assertEqual("system", new_f.category)
        self.assertEqual(fields.MISSING_TIME_SPAN, new_f.time_span)
        self.assertEqual(fields.MISSING_NODE_NAME, new_f.node_name)
        self.assertEqual(fields.MISSING_GRID_ID, new_f.grid_id)
        
        new_f.inherit_missing_from(old_f)
        self.assertEqual("2001293881", new_f.case_num)
        self.assertEqual("2.5.1", new_f.sg_ver)
        self.assertEqual("Sandhawk", new_f.platform)
        self.assertEqual("system", new_f.category)
        self.assertEqual("2017-2018", new_f.time_span)
        self.assertEqual("london", new_f.node_name)
        self.assertEqual("97683", new_f.grid_id)
    
    def test_inherit_missing_all(self):
        old_f = fields.NodeFields(  case_num="2001293881", sg_ver="2.3.2",
                                    platform="Sandhawk", category="bycast",
                                    time_span="2017-2018", node_name="london",
                                    grid_id="97683")
        
        new_f = fields.NodeFields()
        self.assertEqual(fields.MISSING_CASE_NUM, new_f.case_num)
        self.assertEqual(fields.MISSING_SG_VER, new_f.sg_ver)
        self.assertEqual(fields.MISSING_PLATFORM, new_f.platform)
        self.assertEqual(fields.MISSING_CATEGORY, new_f.category)
        self.assertEqual(fields.MISSING_TIME_SPAN, new_f.time_span)
        self.assertEqual(fields.MISSING_NODE_NAME, new_f.node_name)
        self.assertEqual(fields.MISSING_GRID_ID, new_f.grid_id)
        
        new_f.inherit_missing_from(old_f)
        self.assertEqual("2001293881", new_f.case_num)
        self.assertEqual("2.3.2", new_f.sg_ver)
        self.assertEqual("Sandhawk", new_f.platform)
        self.assertEqual("bycast", new_f.category)
        self.assertEqual("2017-2018", new_f.time_span)
        self.assertEqual("london", new_f.node_name)
        self.assertEqual("97683", new_f.grid_id)


class ExtractFieldsTestCase(unittest.TestCase):
    """ Test case for extracting different kinds of fields """

    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.makedirs(self.tmp_dir)
        self.assertTrue(os.path.isdir(self.tmp_dir))
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        self.assertTrue(not os.path.exists(self.tmp_dir))

    def test_get_category(self):
        """ Test that expected categories are matched from file paths """
        # Map sample paths to their "correct" answer
        test_paths = [
            ("scratch_space/950284-vhamemimmgws01-20130713153017-20130713160517/950284/"
             "vhamemimmgws01/20130713153017-20130713160517/mandatory_files/bycast.log", "bycast"),

            ("logjam/scratch_space/950194-vhairoimmsn02-20140520230753-20140520234253/950194/"
             "vhairoimmsn02/20140520230753-20140520234253/mandatory_files/"
             "servermanager.log", "server_manager"),

            ("logjam/scratch_space/950194-vhairoimmsn02-20140520230753-20140520234253/950194/"
             "vhairoimmsn02/20140520230753-20140520234253/system_commands", "system_commands"),

            ("logjam/scratch_space/950194-vhairoimmsn02-20140520215500-20140520231300/950194/"
             "vhairoimmsn02/20140520215500-20140520231300", fields.MISSING_CATEGORY),

            ("asdf123.log", fields.MISSING_CATEGORY),

            ("logjam/scratch_space/950166-vhanflimmcn10-20140717025500-20140717040000/950166/"
             "vhanflimmcn10/20140717025500-20140717040000/mandatory_files/messages", "messages"),
            ]

        for path, correct_category in test_paths:
            self.assertEqual(fields.get_category(path), correct_category)

    def test_get_case_number(self):
        """ Tests that we can properly identify directories with 10-digit case numbers """

        # Dummy paths based on real inputs. These are not read or written to.
        valid_paths = [
            "2004144146",
            "2004436294",
            "2004913956",
            "/mnt/nfs/2001392039",
            "/2004920192"
            ]
        for path in valid_paths:
            case_num = fields.get_case_number(path)
            self.assertNotEqual(fields.MISSING_CASE_NUM, case_num, "Should have found case number %s" % path)

        invalid_paths = [
            "/mnt",
            "/mnt/nfs",
            "asdfasdf",
            "/",
            "/mnt/nfs/12345",
            ]
        for path in invalid_paths:
            case_num = fields.get_case_number(path)
            self.assertEqual(fields.MISSING_CASE_NUM, case_num, "Shouldn't have found case number %s" % path)

    def test_get_version(self):
        try:
            version = fields.get_storage_grid_version(os.path.join(TEST_DATA_DIR,'1234567890'))
            self.assertEqual(version, fields.MISSING_SG_VER)
        except Exception as exc:
            self.fail(exc)

        try:
            lumber_dir = os.path.join(TEST_DATA_DIR, "2234567890", "grid_id_293977", "node_name_paris", "2018-2019")
            version = fields.get_storage_grid_version(lumber_dir)
            self.assertEqual(version, '100.100.100-12345678.0224.asdfg12345')
        except Exception as exc:
            self.fail(exc)

        try:
            version = fields.get_storage_grid_version(os.path.join(TEST_DATA_DIR,'null'))
            self.assertEqual(version, fields.MISSING_SG_VER)
        except Exception as exc:
            self.fail(exc)
    
    def test_get_platform(self):
        try:
            platform = fields.get_platform(os.path.join(TEST_DATA_DIR, "nonexistant_dir"))
            self.assertEqual(fields.MISSING_PLATFORM, platform)
        except Exception as exc:
            self.fail(exc)
    
    def test_get_time_span(self):
        try:
            bad_dir = os.path.join(TEST_DATA_DIR, "ABC-ABC")
            time_span = fields.get_time_span(bad_dir)
            self.assertEqual(fields.MISSING_TIME_SPAN, time_span)
        except Exception as exc:
            self.fail(exc)
        
        try:
            bad_dir = os.path.join(TEST_DATA_DIR, "99992039")
            time_span = fields.get_time_span(bad_dir)
            self.assertEqual(fields.MISSING_TIME_SPAN, time_span)
        except Exception as exc:
            self.fail(exc)

        try:                                # this dir does exist & it extracts from path
            lumber_dir = os.path.join(TEST_DATA_DIR, "2234567890", "grid_id_293977", "node_name_paris", "2018-2019")
            time_span = fields.get_time_span(lumber_dir)
            self.assertEqual("2018-2019", time_span)
        except Exception as exc:
            self.fail(exc)
        
        try:                                # this dir does not exist, extracts from path
            lumber_dir = os.path.join(TEST_DATA_DIR, "2234567890", "grid_id_293977", "node_name_paris", "2015-2017")
            time_span = fields.get_time_span(lumber_dir)
            self.assertEqual("2015-2017", time_span)
        except Exception as exc:
            self.fail(exc)
    
    def test_get_node_name(self):
        try:                                # this dir does exist & it extracts from path
            lumber_dir = os.path.join(TEST_DATA_DIR, "2234567890", "grid_id_293977", "node_name_paris", "2018-2019")
            node_name = fields.get_node_name(lumber_dir)
            self.assertEqual("node_name_paris", node_name)
        except Exception as exc:
            self.fail(exc)
    
    def test_get_grid_id(self):
        try:                                # this dir does exist & it extracts from path
            lumber_dir = os.path.join(TEST_DATA_DIR, "2234567890", "grid_id_293977", "node_name_paris", "2018-2019")
            grid_id = fields.get_grid_id(lumber_dir)
            self.assertEqual("grid_id_293977", grid_id)
        except Exception as exc:
            self.fail(exc)
    
    def test_extract_fields_only_version(self):
        lumber_dir = os.path.join(self.tmp_dir, "gridid_542839", "nodename_london", "2015-2017")
        os.makedirs(lumber_dir)
        self.assertTrue(os.path.isdir(lumber_dir))
        
        lumber_file = os.path.join(lumber_dir, "lumberjack.log")
        with open(lumber_file, "w") as fd:
            fd.write("lumberjack.log file!"+"\n")
        self.assertTrue(os.path.isfile(lumber_file))
            
        sys_file = os.path.join(lumber_dir, "system_commands")
        with open(sys_file, "w") as fd:
            fd.write("storage-grid-release-100.100.100-12345678.0224.asdfg12345"+"\n")
            fd.write("random garbage text")
        self.assertTrue(os.path.isfile(sys_file))
        
        old_f = fields.NodeFields(case_num="2001399485")
        self.assertEqual("2001399485", old_f.case_num)
        
        new_f = fields.extract_fields(lumber_dir, inherit_from=old_f)
        self.assertEqual("2001399485", new_f.case_num)
        self.assertEqual("100.100.100-12345678.0224.asdfg12345", new_f.sg_ver)
        self.assertEqual(fields.MISSING_PLATFORM, new_f.platform)
        self.assertEqual(fields.MISSING_CATEGORY, new_f.category)
        self.assertEqual("2015-2017", new_f.time_span)
        self.assertEqual("nodename_london", new_f.node_name)
        self.assertEqual("gridid_542839", new_f.grid_id)

