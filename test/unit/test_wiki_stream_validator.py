import unittest
import json
from src.wiki_geo_event_gatherer import validate_wiki_json_object


# Unit Tests
class TestWikiGeoValidator(unittest.TestCase):
    # It should return true with a correctly formatted wiki stream result
    def test_correct_format_wiki_file(self):
        correct_test_wiki_file = open("correct_wiki_event.json")
        correct_test_wiki_data = json.load(correct_test_wiki_file)
        correct_test_wiki_file.close()
        result = validate_wiki_json_object(correct_test_wiki_data)
        self.assertTrue(result)

    # It should return false with a wiki result with missing required fields
    def test_wiki_result_missing_required_fields(self):
        missing_fields_wiki_file = open("missing_fields_event.json")
        missing_fields_wiki_data = json.load(missing_fields_wiki_file)
        missing_fields_wiki_file.close()
        result = validate_wiki_json_object(missing_fields_wiki_data)
        self.assertFalse(result)

    # It should return false with a wiki result with all fields present but a wrong field type
    def test_wiki_present_fields_incorrect_type(self):
        wrong_field_type_wiki_file = open("wrong_fields_event.json")
        wrong_field_type_wiki_data = json.load(wrong_field_type_wiki_file)
        wrong_field_type_wiki_file.close()
        result = validate_wiki_json_object(wrong_field_type_wiki_data)
        self.assertFalse(result)
