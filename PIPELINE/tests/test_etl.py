"""Unit tests for ETL functionality."""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl.transform import row_to_dict, determine_person_labels


class TestTransform(unittest.TestCase):
    """Test cases for transform module functions."""
    
    def test_row_to_dict_with_none(self):
        """Test row_to_dict with None input."""
        result = row_to_dict(None)
        self.assertEqual(result, {})
    
    def test_row_to_dict_with_dict(self):
        """Test row_to_dict with dictionary input."""
        test_data = {"id": 1, "name": "Test", "nested": {"key": "value"}}
        result = row_to_dict(test_data)
        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "Test")
        self.assertTrue(isinstance(result["nested"], str))  # Should be converted to JSON string
    
    def test_row_to_dict_with_row(self):
        """Test row_to_dict with Row-like object."""
        class MockRow:
            def asDict(self):
                return {"id": 1, "name": "Test"}
        
        row = MockRow()
        result = row_to_dict(row)
        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "Test")
    
    @patch('src.etl.transform.check_person_role')
    def test_determine_person_labels_primary_only(self, mock_check):
        """Test determine_person_labels for primary applicant only."""
        # Configure the mock to return True for primary_applicant and False for others
        def side_effect(spark, person_id, role_type):
            return role_type == "primary_applicant"
        
        mock_check.side_effect = side_effect
        
        mock_spark = MagicMock()
        result = determine_person_labels(mock_spark, "test_id")
        
        self.assertEqual(result, "Person:PrimaryApplicant")
    
    @patch('src.etl.transform.check_person_role')
    def test_determine_person_labels_all_roles(self, mock_check):
        """Test determine_person_labels for a person with all roles."""
        # Configure the mock to return True for all roles
        mock_check.return_value = True
        
        mock_spark = MagicMock()
        result = determine_person_labels(mock_spark, "test_id")
        
        self.assertEqual(
            result, 
            "Person:PrimaryApplicant:CoApplicant:Reference"
        )


class TestExtract(unittest.TestCase):
    """Test cases for extract module functions."""
    
    @patch('src.etl.extract.read_collection')
    def test_extract_data(self, mock_read_collection):
        """Test extract_data function."""
        # This would be a more involved test that would mock the SparkSession
        # and its behavior - leaving as a placeholder for now
        pass


if __name__ == '__main__':
    unittest.main()