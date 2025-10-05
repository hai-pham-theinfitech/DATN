from vietnamadminunits import parse_address

def parse_address_company(address, type: str = "full"):
    result = parse_address(address)

    address_dict = {
        'street': result.street,
        'ward': result.ward,
        'district': result.district,
        'province': result.province
    }
    if type == "full":
        return address_dict
    elif type == "province":
        return address_dict['province']
    elif type == "district":
        return address_dict['district']
    elif type == "ward":
        return address_dict['ward']
    elif type == "street":
        return address_dict['street']
    else:
        print("Nhập sai type")

