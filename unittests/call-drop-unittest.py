import datetime
import unittest2

from helpers.call_drop_helper import CallDropHelper
from nw_constants.constants import CALL_DROP_TEST_DATA_FILE_PATH


class CallDropTestCase(unittest2.TestCase):

    def setUp(self):
        self.call_drop_obj = CallDropHelper('919860465205', filepath=CALL_DROP_TEST_DATA_FILE_PATH,
                                            sr_date=datetime.datetime.strptime("21-04-2020", "%d-%m-%Y"))

    def test_base_view(self):
        data = self.call_drop_obj.get_msisdn_base_view()
        data.show()
        actual_result = data.rdd.map(tuple).collect()
        print actual_result
        expected_output = [(919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 17, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 18, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 10, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 11, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 4, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 14, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 17, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 18, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 10, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 11, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 4, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 14, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 17, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 18, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 10, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 11, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 4, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 14, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 17, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 18, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 10, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 11, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 4, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 14, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 17, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 18, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 10, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 11, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 4, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 14, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 17, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 18, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 10, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 11, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 4, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 14, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 17, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 18, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 10, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 11, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 4, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 14, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 17, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 18, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 10, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 11, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 4, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 14, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 15, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 1, u'Success'), (919860465205L, datetime.datetime(2020, 4, 17, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 18, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 10, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 11, 0, 0), 1, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 16, 0, 0), 3, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 2, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 13, 0, 0), 4, u'Drop'), (919860465205L, datetime.datetime(2020, 4, 14, 0, 0), 2, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success'), (919860465205L, datetime.datetime(2020, 4, 12, 0, 0), 3, u'Success')]
        self.assertEquals(actual_result, expected_output)

    def tearDown(self):
        self.call_drop_obj.stop_session()


if __name__ == '__main__':
    unittest2.main()