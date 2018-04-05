import sys
import os
if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')

import pp_api.pp_calls as poolparty
from configs.ppconfigs import *


def extract_words(x, debug=True):
    ppinstance = poolparty.PoolParty(server=pp_server, auth_data=auth_data)
    X = str(x).encode('utf8')
    r = ppinstance.extract(text=X, pid=pid)
    cpts = ppinstance.get_cpts_from_response(r)
    reply = []
    for cpt in cpts:
        reply.append((cpt['uri'], 1))
    sp = x.split()
    if debug:
        print("\n\n", "->", len(x), " :: ", x, "\n")
    return reply


def count_words(x, debug=True):
    sp = x.split()
    if debug:
        print("\n\n", "->", len(x), " :: ", x, "\n")
    return [(word, 1) for word in sp]