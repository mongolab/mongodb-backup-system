from datetime import datetime, timedelta

import mbs.schedule as schedule

from mbs.date_utils import epoch_date

from . import BaseTest
from mbs.schedule import AbstractSchedule, Schedule, CronSchedule


###############################################################################
# CronScheduleTest
###############################################################################
class CronScheduleTest(BaseTest):

    ###########################################################################
    def test_make(self):
        obj = self.maker.make({'_type': 'CronSchedule'})
        self.assertIsInstance(obj, AbstractSchedule)
        self.assertIsInstance(obj, CronSchedule)

    ###########################################################################
    def test_validate(self):
        obj = self.maker.make({'_type': 'CronSchedule'})
        errors = obj.validate()
        self.assertEqual(len(errors), 1)
        self.assertIn('missing expression', errors[0])

        obj = self.maker.make({'_type': 'CronSchedule',
                               'expression': 'blah blah blah blah blah'})
        errors = obj.validate()
        self.assertEqual(len(errors), 1)
        self.assertIn('invalid expression', errors[0])

        obj = self.maker.make({'_type': 'CronSchedule',
                               'expression': '*/5 * * *'})
        errors = obj.validate()
        self.assertEqual(len(errors), 1)
        self.assertIn('invalid expression', errors[0])

        # shortcuts like "@monthly are not supported"
        obj = self.maker.make({'_type': 'CronSchedule',
                               'expression': '@monthly'})
        errors = obj.validate()
        self.assertEqual(len(errors), 1)
        self.assertIn('invalid expression', errors[0])

        obj = self.maker.make({'_type': 'CronSchedule',
                               'expression': '*/5 * * * *'})
        errors = obj.validate()
        self.assertEqual(len(errors), 0)

    ###########################################################################
    def test_against_frequency(self):
        cron_sched = self.maker.make({'_type': 'CronSchedule',
                                      'expression': '*/5 * * * *'})
        sched = self.maker.make({'_type': 'Schedule',
                                 'frequency_in_seconds': 300})

        # test period
        cron_occurrences = cron_sched.natural_occurrences_between(
                                epoch_date(),
                                epoch_date() + timedelta(days=2, minutes=7))
        sched_occurrences = sched.natural_occurrences_between(
                                epoch_date(),
                                epoch_date() + timedelta(days=2, minutes=7))
        self.assertSequenceEqual(cron_occurrences, sched_occurrences)

        # test next on occurrence
        self.assertEqual(
            cron_sched.next_natural_occurrence(datetime(2012, 10, 1)),
            sched.next_natural_occurrence(datetime(2012, 10, 1)))

        # test next off occurrence
        self.assertEqual(
            cron_sched.next_natural_occurrence(datetime(2012, 10, 1, 3, 5, 2)),
            sched.next_natural_occurrence(datetime(2012, 10, 1, 3, 5, 2)))

        # test last on occurrence
        self.assertEqual(
            cron_sched.last_natural_occurrence(datetime(2012, 10, 1)),
            sched.last_natural_occurrence(datetime(2012, 10, 1)))

        # test last off occurrence
        self.assertEqual(
            cron_sched.last_natural_occurrence(datetime(2012, 10, 1, 3, 5, 2)),
            sched.last_natural_occurrence(datetime(2012, 10, 1, 3, 5, 2)))

    ###########################################################################
    def test_max_acceptable_lag(self):
        cron_sched = self.maker.make({'_type': 'CronSchedule',
                                      'expression': '*/5 * * * *'})
        self.assertEqual(
            cron_sched._max_acceptable_lag_for_period(timedelta(minutes=5)),
            cron_sched.max_acceptable_lag(datetime(2012, 10, 1)))
        self.assertEqual(
            cron_sched._max_acceptable_lag_for_period(timedelta(minutes=5)),
            cron_sched.max_acceptable_lag(datetime(2012, 10, 1, 3, 5, 2)))

        # test non-constant frequency
        cron_sched = self.maker.make({'_type': 'CronSchedule',
                                      'expression': '0 2 * * 1,2'})
        self.assertEqual(
            cron_sched._max_acceptable_lag_for_period(timedelta(6)),
            cron_sched.max_acceptable_lag(datetime(2012, 10, 8, 2)))
        self.assertEqual(
            cron_sched._max_acceptable_lag_for_period(timedelta(6)),
            cron_sched.max_acceptable_lag(datetime(2012, 10, 8, 1)))
        self.assertEqual(
            cron_sched._max_acceptable_lag_for_period(timedelta(1)),
            cron_sched.max_acceptable_lag(datetime(2012, 10, 8, 3)))

