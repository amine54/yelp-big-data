import os

# Define the content as a multi - line string
business = '''{"business_id":"Pns2l4eNsfO8kk83dixA6A","name":"Abby Rappoport, LAC, CMQ","address":"1616 Chapala St, Ste 2","city":"Santa Barbara","state":"CA","postal_code":"93101","latitude":34.4266787,"longitude":-119.7111968,"stars":5.0,"review_count":7,"is_open":0,"attributes":{"ByAppointmentOnly":"True"},"categories":"Doctors, Traditional Chinese Medicine, Naturopathic/Holistic, Acupuncture, Health & Medical, Nutritionists","hours":null}
{"business_id":"mpf3x - BjTdTEA3yCZrAYPw","name":"The UPS Store","address":"87 Grasso Plaza Shopping Center","city":"Affton","state":"MO","postal_code":"63123","latitude":38.551126,"longitude":-90.335695,"stars":3.0,"review_count":15,"is_open":1,"attributes":{"BusinessAcceptsCreditCards":"True"},"categories":"Shipping Centers, Local Services, Notaries, Mailbox Centers, Printing Services","hours":{"Monday":"0:0 - 0:0","Tuesday":"8:0 - 18:30","Wednesday":"8:0 - 18:30","Thursday":"8:0 - 18:30","Friday":"8:0 - 18:30","Saturday":"8:0 - 14:0"}}
{"business_id":"tUFrWirKiKi_TAnsVWINQQ","name":"Target","address":"5255 E Broadway Blvd","city":"Tucson","state":"AZ","postal_code":"85711","latitude":32.223236,"longitude":-110.880452,"stars":3.5,"review_count":22,"is_open":0,"attributes":{"BikeParking":"True","BusinessAcceptsCreditCards":"True","RestaurantsPriceRange2":"2","CoatCheck":"False","RestaurantsTakeOut":"False","RestaurantsDelivery":"False","Caters":"False","WiFi":"u'no'","BusinessParking":"{'garage': False, 'street': False, 'validated': False, 'lot': True, 'valet': False}","WheelchairAccessible":"True","HappyHour":"False","OutdoorSeating":"False","HasTV":"False","RestaurantsReservations":"False","DogsAllowed":"False","ByAppointmentOnly":"False"},"categories":"Department Stores, Shopping, Fashion, Home & Garden, Electronics, Furniture Stores","hours":{"Monday":"8:0 - 22:0","Tuesday":"8:0 - 22:0","Wednesday":"8:0 - 22:0","Thursday":"8:0 - 22:0","Friday":"8:0 - 23:0","Saturday":"8:0 - 23:0","Sunday":"8:0 - 22:0"}}'''



# Define the folder path where you want to create the file
folder_path = '/home/ibrahim/mock_data'  # Replace with your actual folder path

# Check if the folder exists, if not, create it
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Define the file path
file_path = os.path.join(folder_path, 'business.json')

# Write the content to the file
with open(file_path, 'w') as file:
    file.write(business)

print(f"File created at {file_path}")
