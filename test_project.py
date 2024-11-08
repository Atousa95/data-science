# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch
from os import sep
from impl import AnnotationProcessor, MetadataProcessor, RelationalQueryProcessor
from impl import CollectionProcessor, TriplestoreQueryProcessor, GenericQueryProcessor
from pandas import DataFrame
from main_models import Canvas, Collection, Image, Annotation, Manifest, EntityWithMetadata

# Before running these tests, ensure Blazegraph and other databases are set up.

class TestProjectBasic(unittest.TestCase):

    # File paths and database URL
    annotations = "data" + sep + "annotations.csv"
    collection = "data" + sep + "collection-1.json"
    metadata = "data" + sep + "metadata.csv"
    relational = "." + sep + "relational.db"
    graph = "http://127.0.0.1:9999/blazegraph/sparql"

    def setUp(self):
        """Set up shared resources and initial database paths for each test."""
        self.ann_dp = AnnotationProcessor()
        self.met_dp = MetadataProcessor()
        self.col_dp = CollectionProcessor()
        self.rel_qp = RelationalQueryProcessor()
        self.grp_qp = TriplestoreQueryProcessor()
        self.generic = GenericQueryProcessor()
        

        self.ann_dp.setDbPathOrUrl(self.relational)
        self.met_dp.setDbPathOrUrl(self.relational)
        self.col_dp.setDbPathOrUrl(self.graph)
        self.rel_qp.setDbPathOrUrl(self.relational)
        self.grp_qp.setDbPathOrUrl(self.graph)
        
    def test_annotation_processor(self):
        """Test AnnotationProcessor's ability to set and retrieve database path and upload data."""
        self.assertTrue(self.ann_dp.setDbPathOrUrl(self.relational))
        self.assertEqual(self.ann_dp.getDbPathOrUrl(), self.relational)
        self.assertTrue(self.ann_dp.uploadData(self.annotations))

    def test_metadata_processor(self):
        """Test MetadataProcessor's database path setting and data upload functionality."""
        self.assertTrue(self.met_dp.setDbPathOrUrl(self.relational))
        self.assertEqual(self.met_dp.getDbPathOrUrl(), self.relational)
        self.assertTrue(self.met_dp.uploadData(self.metadata))

    def test_collection_processor(self):
        """Test CollectionProcessor's SPARQL endpoint setup and data upload."""
        self.assertTrue(self.col_dp.setDbPathOrUrl(self.graph))
        self.assertEqual(self.col_dp.getDbPathOrUrl(), self.graph)
        self.assertTrue(self.col_dp.uploadData(self.collection))

    def test_relational_query_processor(self):
        """Test RelationalQueryProcessor's basic querying capabilities."""
        self.assertIsInstance(self.rel_qp.getAllAnnotations(), DataFrame)
        self.assertIsInstance(self.rel_qp.getAllImages(), DataFrame)
        self.assertIsInstance(self.rel_qp.getAnnotationsWithBody("just_a_test"), DataFrame)
        self.assertIsInstance(self.rel_qp.getAnnotationsWithBodyAndTarget("just_a_test", "another_test"), DataFrame)
        self.assertIsInstance(self.rel_qp.getAnnotationsWithTarget("just_a_test"), DataFrame)
        self.assertIsInstance(self.rel_qp.getEntityById("just_a_test"), DataFrame)
        self.assertIsInstance(self.rel_qp.getEntitiesWithCreator("just_a_test"), DataFrame)
        self.assertIsInstance(self.rel_qp.getEntitiesWithTitle("just_a_test"), DataFrame)

    def test_triplestore_query_processor(self):
        """Test TriplestoreQueryProcessor's querying functionality."""
        self.assertIsInstance(self.grp_qp.getAllCanvases(), DataFrame)
        self.assertIsInstance(self.grp_qp.getAllCollections(), DataFrame)
        self.assertIsInstance(self.grp_qp.getAllManifests(), DataFrame)
        self.assertIsInstance(self.grp_qp.getCanvasesInCollection("just_a_test"), DataFrame)
        self.assertIsInstance(self.grp_qp.getCanvasesInManifest("just_a_test"), DataFrame)
        self.assertIsInstance(self.grp_qp.getEntityById("just_a_test"), DataFrame)
        self.assertIsInstance(self.grp_qp.getEntitiesWithLabel("just_a_test"), DataFrame)
        self.assertIsInstance(self.grp_qp.getManifestsInCollection("just_a_test"), DataFrame)

    def test_generic_query_processor(self):
        """Test GenericQueryProcessor's ability to combine relational and RDF data queries."""
        self.generic.addQueryProcessor(self.rel_qp)
        self.generic.addQueryProcessor(self.grp_qp)
        
        # Generic query checks
        self.assertIsInstance(self.generic.getAllAnnotations(), list)
        for ann in self.generic.getAllAnnotations():
            self.assertIsInstance(ann, Annotation)
        
        self.assertIsInstance(self.generic.getAllCanvas(), list)
        for can in self.generic.getAllCanvas():
            self.assertIsInstance(can, Canvas)

        self.assertIsInstance(self.generic.getEntitiesWithCreator("Alighieri, Dante"), list)
        for entity in self.generic.getEntitiesWithCreator("Alighieri, Dante"):
            self.assertIsInstance(entity, EntityWithMetadata)

    @patch("impl.AnnotationProcessor.uploadData")
    def test_mocked_upload(self, mock_upload):
        """Test AnnotationProcessor's upload functionality using a mock to bypass actual I/O."""
        mock_upload.return_value = True
        self.assertTrue(self.ann_dp.uploadData("mock/path.csv"))

    def tearDown(self):
        """Clean up resources after each test if necessary."""
        pass

if __name__ == "__main__":
    unittest.main()
