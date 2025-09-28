from vn_address_converter import convert_to_new_address, Address
from vietnamadminunits import parse_address

def parse_address_company(address: str):
    result = parse_address(address)

    address_dict = {
        'street': result.street,      
        'ward': result.ward,         
        'district': result.district,  
        'province': result.province   
    }
    
    if result.district:  
        address_obj = Address(
            street_address=result.street,
            ward=result.ward,
            district=result.district,
            province=result.province
        )
        
        new_address = convert_to_new_address(address_obj)
        

        address_dict['street'] = new_address.street_address
        address_dict['ward'] = new_address.ward
        address_dict['district'] = new_address.district
        address_dict['province'] = new_address.province
        
    return address_dict