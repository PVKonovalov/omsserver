import random
import time

import requests

loadUrl = 'http://kladr-api.ru/api.php?cityId=5300400021000&contentType=street&offset={}&limit=100'

desktop_agents = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0']


def random_headers():
    return {'User-Agent': random.choice(desktop_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}


def processTransaction(trns, num):
    with open('street{}.sql'.format(num), 'w') as f:
        f.write('-- -----------------------------------------------------\n')
        f.write('-- Data for table `catalog_dkrealtydb`.`street`\n')
        f.write('-- -----------------------------------------------------\n')
        f.write('START TRANSACTION;\n')
        f.write("USE `catalog_dkrealtydb`;\n")

        index = 1
        for item in trns:
            f.write(
                "INSERT INTO `catalog_dkrealtydb`.`street`(`id`, `name`, `locality_id`, `zip`, `type`, `type_short`, `kladr_id`) VALUES({}, '{}', {}, '{}', '{}', '{}', '{}');\n".format(
                    num + index,
                    item['name'], 541, item['zip'], item['type'], item['typeShort'], item['id']))
            index += 1

        f.write('COMMIT;\n')
        f.close()

    print(trns)
    return None


def requestJson(url):
    out = []
    start = 0
    while True:
        resp = requests.get(url.format(start), headers=random_headers())

        if resp.status_code == 404:
            break

        if resp.status_code != 200:
            print('GET {} {}'.format(resp.status_code, resp.content))
            break

        out = resp.json()['result']

        processTransaction(out, start)

        start += 100

        if len(resp.json()['result']) != 100:
            break

        time.sleep(7 + int(random.random() * 5))

        print(start)

    return out


requestJson(loadUrl)
