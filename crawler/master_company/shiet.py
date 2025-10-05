import requests
import json

# Định nghĩa URL API và mã số thuế cần tra cứu
url = "https://api.vietqr.io/v2/business/011110381"

# Gửi yêu cầu GET đến API
response = requests.get(url)

# Kiểm tra nếu yêu cầu thành công
if response.status_code == 200:
    # Phân tích dữ liệu JSON trả về
    data = response.json()
    
    # Kiểm tra nếu mã trạng thái trả về là '00' (thành công)
    if data['code'] == '00':
        tax_id = data['data']['id']
        name = data['data']['name']
        address = data['data']['address']
        international_name = data['data']['internationalName']
        
        print(f"Mã số thuế: {tax_id}")
        print(f"Tên công ty: {name}")
        print(f"Địa chỉ: {address}")
        print(f"Tên quốc tế: {international_name}")
    else:
        print(f"Lỗi: {data['desc']}")
else:
    print(f"Yêu cầu thất bại. Mã lỗi: {response.status_code}")
