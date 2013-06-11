import mbs.schedule as schedule

from . import BaseTest
from mbs.schedule import AbstractSchedule, Schedule, CronSchedule


class CronScheduleTest(BaseTest):
    def test_make(self):
        obj = self.maker.make({'_type': 'CronSchedule'})
        self.assertIsInstance(obj, AbstractSchedule)
        self.assertIsInstance(obj, CronSchedule)

