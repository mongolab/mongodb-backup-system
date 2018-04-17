import calendar
from datetime import datetime, timedelta, date

###############################################################################
def date_now():
    return datetime.utcnow()

###############################################################################
def seconds_now():
    return date_to_seconds(date_now())

###############################################################################
def epoch_date():
    return datetime(1970, 1, 1)

###############################################################################
def date_to_seconds(date):
    return calendar.timegm(date.timetuple())

###############################################################################
def seconds_to_date(seconds):
    return datetime.utcfromtimestamp(seconds)

###############################################################################
def date_plus_seconds(date, seconds):
    return seconds_to_date(date_to_seconds(date) + seconds)

###############################################################################
def date_minus_seconds(date, seconds):
    return seconds_to_date(date_to_seconds(date) - seconds)

###############################################################################
def mid_date_between(d1, d2):
    return d1 + (d2 - d1)/2

###############################################################################
def yesterday_date():
    return today_date() - timedelta(days=1)

###############################################################################
def today_date():
    return date_now().replace(hour=0, minute=0, second=0, microsecond=0)

###############################################################################
def is_date_value(value):
    return type(value) in [datetime, date]

###############################################################################
def timedelta_total_seconds(td):
    """
    Equivalent python 2.7+ timedelta.total_seconds()
     This was added for python 2.6 compatibility
    """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6


###############################################################################
def days_in_month(date):
    monthRange = calendar.monthrange(date.year, date.month)
    return monthRange[1]

###############################################################################
def string_to_datetime( date_str ):
    return datetime( *map( int, date_str.split('.') ))

###############################################################################
def datetime_to_day_string(ts):
    parts = datetime_to_parts_list( ts )
    return ".".join(map(str,parts[:3]))

###############################################################################
def datetime_to_string( ts ):
    parts = datetime_to_parts_list( ts )
    # strip out the smaller units if they are 0
    for i in range(len(parts)-1, 0, -1):
        if parts[i] == 0:
            del parts[i]
        else:
            break
    return ".".join( map( str, parts ) )

###############################################################################
def datetime_to_parts_list( ts ):
    return [ ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second,
             ts.microsecond ]

###############################################################################
def datetime_to_bson( ts ):
    return { "$date" : ts.strftime( "%Y-%m-%dT%H:%M:%S.000Z" ) }

###############################################################################
def datetime_to_iso_str( ts ):
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

###############################################################################
def utc_str_to_datetime( str ):
    return datetime.strptime(str, "%Y-%m-%dT%H:%M:%S.000Z")

###############################################################################
def time_str_to_datetime_today(str):
    time =  datetime.strptime(str, "%H:%M")
    today = today_date()
    return today.replace(hour=time.hour, minute=time.minute)

def time_string(time_seconds):
    days, remainder = divmod(time_seconds, 3600 * 24)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    result = []
    if days:
        result.append("%d day(s)" % days)
    if days or hours:
        result.append("%d hour(s)" % hours)
    if days or hours or minutes:
        result.append("%d minute(s)" % minutes)

    result.append("%d second(s)" % seconds)

    return " ".join(result)

def days_ago_to_dt(days_from_now):
    current_dt = date_now()
    result = current_dt - timedelta(days=days_from_now)
    return result