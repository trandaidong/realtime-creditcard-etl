import pandas as pd
import random
from datetime import datetime, timedelta
import faker

fake = faker.Faker()
# Cấu hình số lượng dòng dữ liệu
num_rows = 112340
unique_users = [fake.name() for _ in range(12345)]

# Gán card duy nhất cho mỗi người
user_card_map = {user: random.randint(10**17, 10**18 - 1) for user in unique_users}
# Các giá trị mẫu để random
merchant_names = ['Walmart', 'Target', 'Starbucks', 'Amazon', 'Shell', 'Best Buy', 'Costco', '7-Eleven']
cities = ['Monterey Park', 'La Verne', 'Mira Loma', 'Los Angeles', 'Pasadena']
states = ['CA', 'NV', 'TX', 'NY']
mccs = [5411.0, 5300.0, 5651.0, 7832.0, 7538.0, 4900.0]
errors = [
    '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 
    '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
    '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 
     '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 
    'Technical Glitch',
    'Incorrect PIN',
    'Card Declined',
    'Expired Card',
    'Insufficient Funds',
    'Invalid Card Number',
    'Card Reported Lost',
    'Transaction Timeout',
    'Suspected Fraud',
    'Card Blocked by Bank',
    'Exceeds Withdrawal Limit',
    'Merchant Not Found',
    'Currency Not Supported',
    'Duplicate Transaction',
    'Invalid CVV',
    'Invalid Expiry Date',
    'Account Closed',
    'Issuer Unavailable'
]
use_chip_types = ['Swipe Transaction', 'Chip Transaction', 'Online Transaction']
zip_codes = [91754.0, 91750.0, 91752.0, 90001.0, 90210.0]

# Khởi tạo thời gian bắt đầu
start_datetime = datetime(2020, 1, 1, 6, 0)

rows = []
for i in range(num_rows):
    user= random.choice(unique_users)
    card=user_card_map[user]

    current_datetime = start_datetime + timedelta(minutes=random.randint(10, 300))
    start_datetime = current_datetime  # Cập nhật để đảm bảo thời gian tăng dần

    amount = f"${round(random.uniform(2.0, 150.0), 2)}"
    merchant = random.choice(merchant_names)
    city = random.choice(cities)
    state = random.choice(states)
    zip_code = random.choice(zip_codes)
    mcc = random.choice(mccs)
    error = random.choice(errors)
    use_chip = random.choice(use_chip_types)
    card_number = random.randint(10**17, 10**18 - 1)

    if error != '':
        is_fraud = random.choices(['Yes', 'No'], weights=[25, 75])[0]  # Nếu có lỗi thì tăng nhẹ xác suất fraud
    else:
        is_fraud = random.choices(['Yes', 'No'], weights=[5, 95])[0]


    rows.append({
        'User': user,
        'Card': card,
        'Year': current_datetime.year,
        'Month': current_datetime.month,
        'Day': current_datetime.day,
        'Time': current_datetime.strftime("%H:%M"),
        'Amount': amount,
        'Use Chip': use_chip,
        'Merchant Name': merchant,
        'Merchant City': city,
        'Merchant State': state,
        'Zip': zip_code,
        'MCC': mcc,
        'Errors?': error,
        'Is Fraud?': is_fraud
    })

# Tạo DataFrame và lưu ra file CSV
df = pd.DataFrame(rows)
df.to_csv("data.csv", index=False)

print("Dữ liệu đã được tạo và lưu vào file 'data.csv'.")
