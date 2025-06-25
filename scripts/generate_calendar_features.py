import sys
from pathlib import Path
import pandas as pd
import exchange_calendars as ecals

# Add src and database to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))
sys.path.append(str(Path(__file__).parent.parent / 'database'))

from database_manager import DatabaseManager

# Fix offset issue: use pd.offsets.Week(2) + Weekday index instead of 'week' arg
from pandas.tseries.offsets import Week

# Updated holiday labeling function
def get_holiday_name(dt):
    y = dt.year
    md = (dt.month, dt.day)
    dt_date = dt.date() if hasattr(dt, 'date') else dt

    # Fixed-date holidays
    if md == (1, 1):
        return "New Year's Day"
    elif md == (6, 19):
        return "Juneteenth National Independence Day"
    elif md == (7, 4):
        return "Independence Day"
    elif md == (12, 25):
        return "Christmas Day"
    elif dt_date == pd.Timestamp("2018-12-05").date():
        return "Bush Funeral (Special Closure)"

    # MLK Day: 3rd Monday in January
    mlk_days = pd.date_range(f"{y}-01-01", f"{y}-01-31", freq="W-MON")
    if len(mlk_days) > 2 and dt_date == mlk_days[2].date():
        return "Martin Luther King Jr. Day"
    
    # Presidents' Day: 3rd Monday in February
    pres_days = pd.date_range(f"{y}-02-01", f"{y}-02-28", freq="W-MON")
    if len(pres_days) > 2 and dt_date == pres_days[2].date():
        return "Presidents' Day"

    # Good Friday (manual map)
    easter_fridays = {
        2018: "2018-03-30",
        2019: "2019-04-19",
        2020: "2020-04-10",
        2021: "2021-04-02",
        2022: "2022-04-15",
        2023: "2023-04-07",
        2024: "2024-03-29",
        2025: "2025-04-18",
        2026: "2026-04-03"
    }
    if str(dt_date) == easter_fridays.get(y):
        return "Good Friday"

    # Memorial Day: last Monday in May
    memorial_day = max(pd.date_range(f"{y}-05-01", f"{y}-05-31", freq="W-MON"))
    if dt_date == memorial_day.date():
        return "Memorial Day"

    # Labor Day: 1st Monday in September
    labor_day = min(pd.date_range(f"{y}-09-01", f"{y}-09-30", freq="W-MON"))
    if dt_date == labor_day.date():
        return "Labor Day"

    # Thanksgiving: 4th Thursday in November
    thanksgiving = pd.date_range(f"{y}-11-01", f"{y}-11-30", freq="W-THU")[3]
    if dt_date == thanksgiving.date():
        return "Thanksgiving Day"

    return "Unknown"

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'database', 'stock_database.db')


def main():
    # First, create calendar of dates, one per day, from 2018-01-01 to 2030-12-31
    start_date = pd.Timestamp('2018-01-01')
    end_date = pd.Timestamp('2030-12-31')
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    calendar_df = pd.DataFrame({'date': date_range})
    calendar_df['date'] = calendar_df['date'].dt.date  # Convert to date type
    print(f"Calendar sample:\n{calendar_df.head()}"
            f"\nTotal dates: {len(calendar_df)}")
    calendar_df['date'] = pd.to_datetime(calendar_df['date'])

    # Attach one-hot-encoded day of week to calendar_df, e.g., dow_1 = 1 for monday, 0 othwerwise       
    calendar_df['dow_1'] = (calendar_df['date'].dt.dayofweek == 0).astype(int)
    calendar_df['dow_2'] = (calendar_df['date'].dt.dayofweek == 1).astype(int)
    calendar_df['dow_3'] = (calendar_df['date'].dt.dayofweek == 2).astype(int)
    calendar_df['dow_4'] = (calendar_df['date'].dt.dayofweek == 3).astype(int)
    calendar_df['dow_5'] = (calendar_df['date'].dt.dayofweek == 4).astype(int)
    calendar_df['dow_6'] = (calendar_df['date'].dt.dayofweek == 5).astype(int)
    calendar_df['dow_7'] = (calendar_df['date'].dt.dayofweek == 6).astype(int)

    # Attach one-hot-encoded month of year to calendar_df, e.g., month_1 = 1 for january, 0 othwerwise
    calendar_df['month_1'] = (calendar_df['date'].dt.month == 1).astype(int)
    calendar_df['month_2'] = (calendar_df['date'].dt.month == 2).astype(int)
    calendar_df['month_3'] = (calendar_df['date'].dt.month == 3).astype(int)
    calendar_df['month_4'] = (calendar_df['date'].dt.month == 4).astype(int)
    calendar_df['month_5'] = (calendar_df['date'].dt.month == 5).astype(int)
    calendar_df['month_6'] = (calendar_df['date'].dt.month == 6).astype(int)
    calendar_df['month_7'] = (calendar_df['date'].dt.month == 7).astype(int)
    calendar_df['month_8'] = (calendar_df['date'].dt.month == 8).astype(int)
    calendar_df['month_9'] = (calendar_df['date'].dt.month == 9).astype(int)
    calendar_df['month_10'] = (calendar_df['date'].dt.month == 10).astype(int)
    calendar_df['month_11'] = (calendar_df['date'].dt.month == 11).astype(int)
    calendar_df['month_12'] = (calendar_df['date'].dt.month == 12).astype(int)  

    # Attach one-hot-encoded quarter of year to calendar_df, e.g., quarter_1 = 1 for Q1, 0 othwerwise
    calendar_df['quarter_1'] = (calendar_df['date'].dt.quarter == 1).astype(int)
    calendar_df['quarter_2'] = (calendar_df['date'].dt.quarter == 2).astype(int)
    calendar_df['quarter_3'] = (calendar_df['date'].dt.quarter == 3).astype(int)
    calendar_df['quarter_4'] = (calendar_df['date'].dt.quarter == 4).astype(int)

    # Process holidays using exchange_calendars
    nyse = ecals.get_calendar('XNYS')
    start_date = pd.Timestamp('2018-01-01', tz='America/New_York')
    end_date = pd.Timestamp('2030-12-31', tz='America/New_York')

    # Get all market holidays in the range
    holidays = nyse.regular_holidays.holidays(start_date, end_date)
    holidays = pd.Series(holidays)
    holidays = holidays[(holidays >= start_date) & (holidays <= end_date)]

    # Convert to DataFrame for display
    holidays_df = pd.DataFrame(holidays, columns=["Holiday"])
    holidays_df.reset_index(drop=True, inplace=True)
    holidays_df["Year"] = holidays_df["Holiday"].dt.year   

    # Apply the corrected function
    holidays_df["Holiday Name"] = holidays_df["Holiday"].apply(get_holiday_name)

    # Display final result
    holidays_df.head(20) 
    print(f"Market holidays from {start_date.date()} to {end_date.date()}:\n{holidays_df}")
    # print 20 random rows of calendar_df
    print(f"Calendar with features sample:\n{calendar_df.sample(20)}")

if __name__ == "__main__":
    main()
