from datetime import datetime, timedelta, timezone, date
from typing import Iterator, Union
from dateutil import parser

def parse_date(date_string: Union[str, datetime]) -> datetime:
    if isinstance(date_string, datetime):
        return date_string.astimezone(timezone.utc)

    if isinstance(date_string, str):
        try:
            dt = parser.isoparse(date_string)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass

        try:
            return datetime.fromtimestamp(float(date_string), tz=timezone.utc)
        except ValueError:
            pass

    raise ValueError(f"Unable to parse date string: {date_string}")


def format_date(date_obj: Union[str, datetime]) -> str:
    """Format a datetime object to an ISO 8601 string."""
    if isinstance(date_obj, str):
        date_obj = parse_date(date_obj)
    return date_obj.astimezone(timezone.utc).isoformat()


def get_start_of_day(date_obj: Union[str, datetime]) -> datetime:
    """Get the start of the day for a given date."""
    if isinstance(date_obj, str):
        date_obj = parse_date(date_obj)
    return date_obj.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def get_end_of_day(date_obj: Union[str, datetime]) -> datetime:
    """Get the end of the day for a given date."""
    if isinstance(date_obj, str):
        date_obj = parse_date(date_obj)
    return date_obj.astimezone(timezone.utc).replace(
        hour=23, minute=59, second=59, microsecond=999999
    )


def date_range(
    start_date: Union[str, datetime], end_date: Union[str, datetime]
) -> Iterator[date]:
    """Generate a range of dates from start_date to end_date, inclusive."""
    start = get_start_of_day(parse_date(start_date))
    end = get_start_of_day(parse_date(end_date))
    while start <= end:
        yield start.date()
        start += timedelta(days=1)


def days_ago(num_days: int) -> datetime:
    """Returns the datetime object for the date num_days ago from now."""
    return datetime.now(timezone.utc) - timedelta(days=num_days)
