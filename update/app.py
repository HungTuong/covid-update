from datetime import datetime
import csv
import requests
import json
import boto3

# import requests

# -*- coding: utf-8 -*-
# unused link
#
# vn_map = "https://s.vnecdn.net/vnexpress/j/v47/event/covid/js/vn-all.js"
# url = "https://gw.vnexpress.net/ar/get_rule_2?category_id=1004765&limit=4&page=1"
# map_df = pd.read_csv("https://vnexpress.net/microservice/sheet/type/covid19_2021_by_map")


# ######
# Normalize header to support graphql

# df.columns[:-2]
columns = ['Ngày', 'Hà Nội', 'TP HCM', 'Hưng Yên', 'Hà Nam', 'Vĩnh Phúc',
           'Đà Nẵng', 'Yên Bái', 'Quảng Nam', 'Đồng Nai', 'Hải Dương', 'Thái Bình',
           'Quảng Ngãi', 'Lạng Sơn', 'Bắc Ninh', 'Thanh Hóa', 'Điện Biên',
           'Nghệ An', 'Nam Định', 'Phú Thọ', 'Quảng Ninh', 'Bắc Giang',
           'Hải Phòng', 'Thừa Thiên Huế', 'Đăk Lăk', 'Hòa Bình', 'Quảng Trị',
           'Tuyên Quang', 'Sơn La', 'Ninh Bình', 'Thái Nguyên', 'Long An',
           'Bạc Liêu', 'Gia Lai', 'Tây Ninh', 'Bình Dương', 'Trà Vinh',
           'Đồng Tháp', 'Hà Tĩnh', 'Tiền Giang', 'Bắc Kạn', 'Lào Cai', 'An Giang',
           'Vĩnh Long', 'Kiên Giang', 'Khánh Hòa', 'Phú Yên', 'Bình Thuận',
           'Cần Thơ', 'Bà Rịa - Vũng Tàu', 'Bình Định', 'Bình Phước', 'Lâm Đồng',
           'Ninh Thuận', 'Bến Tre', 'Sóc Trăng', 'Cà Mau', 'Hậu Giang', 'Đăk Nông',
           'Kon Tum', 'Hà Giang', 'Quảng Bình', 'Lai Châu']


# value = map_df['ENGLISH'].str.replace(' ', '_').str.lower()
# key = map_df['TỈNH THÀNH']
# dict(zip (key,value))
name_dic = {'Lai Châu': 'lai_chau',
            'Tuyên Quang': 'tuyen_quang',
            'Yên Bái': 'yen_bai',
            'Bắc Kạn': 'bac_kan',
            'Quảng Ninh': 'quang_ninh',
            'Thái Nguyên': 'thai_nguyen',
            'Nam Định': 'nam_dinh',
            'Phú Thọ': 'phu_tho',
            'Hòa Bình': 'hoa_binh',
            'Hà Giang': 'ha_giang',
            'Kon Tum': 'kon_tum',
            'Quảng Trị': 'quang_tri',
            'Hải Phòng': 'hai_phong',
            'Cà Mau': 'ca_mau',
            'Quảng Bình': 'quang_binh',
            'Ninh Bình': 'ninh_binh',
            'Lào Cai': 'lao_cai',
            'Thái Bình': 'thai_binh',
            'Điện Biên': 'dien_bien',
            'Bạc Liêu': 'bac_lieu',
            'Hà Nam': 'ha_nam',
            'Sơn La': 'son_la',
            'Thanh Hóa': 'thanh_hoa',
            'Lạng Sơn': 'lang_son',
            'Lâm Đồng': 'lam_dong',
            'Hải Dương': 'hai_duong',
            'Thừa Thiên Huế': 'thua_thien_hue',
            'Đăk Nông': 'dak_nong',
            'Quảng Nam': 'quang_nam',
            'Vĩnh Phúc': 'vinh_phuc',
            'Gia Lai': 'gia_lai',
            'Hà Tĩnh': 'ha_tinh',
            'Bình Phước': 'binh_phuoc',
            'Hưng Yên': 'hung_yen',
            'Hậu Giang': 'hau_giang',
            'Bình Định': 'binh_dinh',
            'Quảng Ngãi': 'quang_ngai',
            'Kiên Giang': 'kien_giang',
            'Nghệ An': 'nghe_an',
            'Sóc Trăng': 'soc_trang',
            'Đăk Lăk': 'dak_lak',
            'Ninh Thuận': 'ninh_thuan',
            'Trà Vinh': 'tra_vinh',
            'An Giang': 'an_giang',
            'Bến Tre': 'ben_tre',
            'Bình Thuận': 'binh_thuan',
            'Vĩnh Long': 'vinh_long',
            'Đà Nẵng': 'da_nang',
            'Bắc Ninh': 'bac_ninh',
            'Phú Yên': 'phu_yen',
            'Cần Thơ': 'can_tho',
            'Hà Nội': 'ha_noi',
            'Bà Rịa - Vũng Tàu': 'ba_ria_vung_tau',
            'Tây Ninh': 'tay_ninh',
            'Tiền Giang': 'tien_giang',
            'Khánh Hòa': 'khanh_hoa',
            'Đồng Tháp': 'dong_thap',
            'Bắc Giang': 'bac_giang',
            'Đồng Nai': 'dong_nai',
            'Long An': 'long_an',
            'Bình Dương': 'binh_duong',
            'TP HCM': 'tp_hcm',
            'Cao Bằng': 'cao_bang',
            'Hoàng Sa': 'hoang_sa',
            'Trường Sa': 'truong_sa',
            'Ngày': 'ngay'
            }

id_map = {'Lai Châu': 'VN-01',
        'Lào Cai': 'VN-02',
        'Hà Giang': 'VN-03',
        'Cao Bằng': 'VN-04',
        'Sơn La': 'VN-05',
        'Yên Bái': 'VN-06',
        'Tuyên Quang': 'VN-07',
        'Lạng Sơn': 'VN-09',
        'Quảng Ninh': 'VN-13',
        'Hòa Bình': 'VN-14',
        'Ninh Bình': 'VN-18',
        'Thái Bình': 'VN-20',
        'Thanh Hóa': 'VN-21',
        'Nghệ An': 'VN-22',
        'Hà Tĩnh': 'VN-23',
        'Quảng Bình': 'VN-24',
        'Quảng Trị': 'VN-25',
        'Thừa Thiên Huế': 'VN-26',
        'Quảng Nam': 'VN-27',
        'Kon Tum': 'VN-28',
        'Quảng Ngãi': 'VN-29',
        'Gia Lai': 'VN-30',
        'Bình Định': 'VN-31',
        'Phú Yên': 'VN-32',
        'Đắk Lắk': 'VN-33',
        'Khánh Hòa': 'VN-34',
        'Lâm Đồng': 'VN-35',
        'Ninh Thuận': 'VN-36',
        'Tây Ninh': 'VN-37',
        'Đồng Nai': 'VN-39',
        'Bình Thuận': 'VN-40',
        'Long An': 'VN-41',
        'Bà Rịa – Vũng Tàu': 'VN-43',
        'An Giang': 'VN-44',
        'Đồng Tháp': 'VN-45',
        'Tiền Giang': 'VN-46',
        'Kiên Giang': 'VN-47',
        'Vĩnh Long': 'VN-49',
        'Bến Tre': 'VN-50',
        'Trà Vinh': 'VN-51',
        'Sóc Trăng': 'VN-52',
        'Bắc Kạn': 'VN-53',
        'Bắc Giang': 'VN-54',
        'Bạc Liêu': 'VN-55',
        'Bắc Ninh': 'VN-56',
        'Bình Dương': 'VN-57',
        'Bình Phước': 'VN-58',
        'Cà Mau': 'VN-59',
        'Hải Dương': 'VN-61',
        'Hà Nam': 'VN-63',
        'Hưng Yên': 'VN-66',
        'Nam Định': 'VN-67',
        'Phú Thọ': 'VN-68',
        'Thái Nguyên': 'VN-69',
        'Vĩnh Phúc': 'VN-70',
        'Điện Biên': 'VN-71',
        'Đắk Nông': 'VN-72',
        'Hậu Giang': 'VN-73',
        'Cần Thơ': 'VN-CT',
        'Đà Nẵng': 'VN-DN',
        'Hà Nội': 'VN-HN',
        'Hải Phòng': 'VN-HP',
        'TP. Hồ Chí Minh': 'VN-SG'}
# -

# #####


# pd.set_option('display.max_rows', None)

def get_data_from(url):
    response = requests.get(url)
    decoded_content = response.content.decode('utf-8')

    data = csv.reader(decoded_content.splitlines(), delimiter=',')
    header = next(data)
    cols = [list(col) for col in zip(*data)]
    return dict(zip(header, cols))


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    s3_object = s3.Object('ncov', 'cov_data.json')

    increase_by_day = get_data_from(
        "https://vnexpress.net/microservice/sheet/type/covid19_2021_281")
    total_by_day = get_data_from(
        "https://vnexpress.net/microservice/sheet/type/covid19_2021_by_location")

    response = requests.get(
        url="https://vnexpress.net/microservice/sheet/type/vaccine_covid_19")
    asean_data = response.json()
    for country_dat in asean_data['data']:
        country_dat.update(country_dat['vaccine'])
        country_dat.pop('vaccine')
        country_dat.pop('')

    response = requests.get(
        url="https://static.pipezero.com/covid/data.json")
    vn_gov_data = response.json()

    for province in vn_gov_data['locations']:
        province['id'] = id_map[province['name']]

    data = {
        'increase_by_day': increase_by_day,
        'total_by_day': total_by_day,
        'asean_data': asean_data,
        'vn_gov_data': vn_gov_data,
        'time': datetime.now().isoformat()
    }

    try:
        s3_object.put(
            ACL='public-read',
            Body=(json.dumps(data, ensure_ascii=False, indent=4))
        )
    except:
        client = boto3.client('ses', region_name='ap-southeast-1')
        response = client.send_email(
            Destination={
                'ToAddresses': ['sunfoxy2k@gmail.com'],
                'CcAddresses': ['hung.tim.cook@gmail.com']
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': 'Something wrong with the updating cov_data.json in ncov S3 Bucket !',
                    },
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': 'Update Error',
                },
            },
            Source='mhung.tuong@gmail.com',
        )
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Cannot update the file",
            }),
        }
    else:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Successfully update cov_data.json at {}".format(datetime.now()),
            }),
        }
