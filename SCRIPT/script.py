import random
import datetime
from snowflake.snowpark import Session

def main(session: Session):
    
    business_climate = 'medium'  
    climate_factor = {'low': 10, 'medium': 50, 'high': 200}[business_climate]
    
   
    statuses = ['Active', 'Inactive', 'Pending', 'Expired']
    for status in statuses:
        session.sql(f"INSERT INTO STATUS (STATUSLABEL) VALUES ('{status}')").collect()
    
  
    for _ in range(climate_factor):
        visitor_number = random.randint(1000, 9999)
        years = random.randint(5, 60)
        weight = random.randint(20, 150)
        height = round(random.uniform(1.0, 2.5), 2)
        session.sql(f"""
            INSERT INTO VISITOR (VISITORNUMBER, YEARS, WEIGHT, HEIGHT) 
            VALUES ({visitor_number}, {years}, {weight}, {height})
        """).collect()
    
  
    attraction_names = ['Roller Coaster', 'Haunted House', 'Ferris Wheel', 'Bumper Cars']
    for name in attraction_names:
        years_constraint = '8+' if 'Coaster' in name else '5+'
        weight_constraint = '50+' if 'Coaster' in name else '20+'
        height_constraint = '1.2+' if 'Coaster' in name else '1.0+'
        description = f"Exciting {name} attraction!"
        session.sql(f"""
            INSERT INTO ATTRACTION (NAME, YEARSCONSTRAINT, WEIGHTCONSTRAINT, HEIGHTCONSTRAINT, DESCRIPTION) 
            VALUES ('{name}', '{years_constraint}', '{weight_constraint}', '{height_constraint}', '{description}')
        """).collect()
    
    
    for i in range(1, 4):
        name = f"Canteen_{i}"
        description = "Food and drinks available"
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        daily_profit = round(random.uniform(10, 999.999), 3) 
        session.sql(f"""
            INSERT INTO CANTEEN (NAME, DESCRIPTION, DATE, DAYILYPROFIT) 
            VALUES ('{name}', '{description}', '{date}', {daily_profit})
        """).collect()

   
    return session.sql("SELECT ' Успешно вмъкнати данни' AS RESULT")
