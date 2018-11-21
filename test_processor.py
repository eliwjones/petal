import processor
import unittest

"""
  Generally prefer py.test, but no reason to require a venv when unittest will do.

"""

class TestProcessor(unittest.TestCase):

    def test_it(self):
        expected = list(processor.extract_csv_data('test-solutions.csv', ','))
        expected_user_ids = set([data['user_id'] for data in expected])

        """
          Data will be dumped to 'aggregated-transactions.csv'

          TODO: More proper output file naming keyed off of processed filepath. 

        """

        processor.process('test-transactions.csv')

        results = processor.extract_csv_data('aggregated-transactions.csv', '|')

        filtered_results = {data['user_id']: data for data in results if data['user_id'] in expected_user_ids}

        for expected_result in expected:
            user_id = expected_result['user_id']

            self.assertEqual(filtered_results[user_id], expected_result)


if __name__ == '__main__':
    unittest.main()
