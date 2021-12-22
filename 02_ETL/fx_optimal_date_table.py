from datetime import date, timedelta
import pandas as pd

def generate_period_table(start_date, end_date):
    
    period_list = []

    date_diff = (end_date - start_date).days

    for num in range(date_diff+1):
        active_date = start_date + timedelta(days=num)
        year = active_date.year
        month_number = active_date.month
        quarter_number = int(((month_number-1)/3)+1)
        long_month_name = active_date.strftime('%B')
        month = active_date.strftime('%b')
        quarter = 'Q' + str(quarter_number)
        month_year = month + '-' + str(year)
        quarter_year = quarter + '-' + str(year)
        month_year_sort = int(str(year)+'0'+str(month_number)) if month_number <= 9 else int(str(year)+ str(month_number))
        quarter_year_sort = int(str(year)+'0'+str(quarter_number))
        
        date_details = {
            'Date': active_date,
            'Year': year,
            'QuarterNumber': quarter_number,
            'MonthNumber': month_number,
            'Long Month Name': long_month_name,
            'Month': month,
            'Quarter': quarter,
            'Month-Year': month_year,
            'Month-YearSort': month_year_sort,
            'Quarter-Year': quarter_year,
            'Quarter-YearSort': quarter_year_sort
        }
        
        period_list.append(date_details)

    dim_period = pd.DataFrame(period_list)
    return dim_period

# -- Testing the Function -- #
# ========================== #

if __name__ == '__main__':
    
    start_date = date (2020, 1, 1)
    end_date = date (2020, 12, 31)
    
    dim_period = generate_period_table(start_date, end_date)
    
    print(dim_period)