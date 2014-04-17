from __future__ import division, print_function


INDEX_URL_PROV = 'http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx'  # noqa
DS_URL_PROV = 'http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?idind={0}&fmt=json'  # noqa
INDEX_URL_SUBPRO = 'http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx'  # noqa


import requests


def color(text, c):
    return "\033[{1}m{0}\033[0m".format(text, c)


class Error(Exception):
    pass


# ----------------------------------------------------------------------
# Indicatori Provinciali


def get_ids_prov():
    resp = requests.get(INDEX_URL_PROV)
    data = resp.json()
    return [int(r['id']) for r in data[data.keys()[0]]]


def dataset_prov_exists(dsid):
    resp = requests.get(DS_URL_PROV.format(dsid))
    if not resp.ok:
        raise Error(str(resp.status_code))
    try:
        resp.json()
    except:
        raise Error('NOJS')
    return True


ids_prov = get_ids_prov()
max_id_prov = max(ids_prov)
for i in xrange(1, int(max_id_prov * 1.5)):
    if i in ids_prov:
        in_list = color('True', '1;32')
    else:
        in_list = color('False', '1;31')

    try:
        dataset_prov_exists(i)
    except Error, e:
        exists = color(e.message, '1;31')
    except Exception:
        exists = color('FAIL', '1;31')
    else:
        exists = color('OK', '1;32')

    print("{0:5d} {1:^20} {2:^20}".format(i, in_list, exists))
