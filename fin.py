#!/usr/bin/env python3
# coding: utf-8

import copy
import numpy as np
import matplotlib as mp
#mp.use('nbagg')
import matplotlib.pyplot as plt
from collections import defaultdict
import sys
import datetime
import dateutil
from pprint import pprint
import json
import logging
import csv

class FinDb(object):
    log = logging.getLogger('FinDb')
    def __init__(self, data_in):
        self.data_in = data_in
        self.accounts = data_in['accounts']
        self.trans_log = []
        self.fill_trans_log(self.data_in['events'])
        self.bal_log = []
        self.fill_bal_log()
        self.fill_bal_log_by_acct_id()

    def fill_trans_log(self, transactions):
        self.trans_log += self._mk_trans_log(transactions)

    def _mk_trans_log(self, transactions, default_when=None):
        if default_when is None:
            #caller is assuring us that when will be specified in data
            pass
        elif isinstance(default_when, str):
            default_when = dateutil.parser.parse(default_when)
        flattened = []
        def determine_when(t):
            #add i-microsecond offset so events in trans chain are not simultaneous
            offset = datetime.timedelta(microseconds=i)
            if 'when' in t:
                when = dateutil.parser.parse(t['when'])
            elif default_when != None:
                when = default_when+offset
            else:
                raise ValueError('default_when not specified and data lacks when: {}'.format(t))
            return when
        #can't use for because we're going to be mutating transactions
        i = 0
        while len(transactions) > 0:
            t = transactions.pop(0)
            t_type = t.get('type', 'trans')
            if 'rrule' in t:
                my_rrule_rec = copy.deepcopy(t['rrule'])
                #datetime-ify some fields
                for f in ['dtstart', 'until']:
                    if f in my_rrule_rec:
                        my_rrule_rec[f] = dateutil.parser.parse(my_rrule_rec[f])
                freq = my_rrule_rec.pop('freq')
                freq = getattr(dateutil.rrule, freq) 
                rr = list(dateutil.rrule.rrule(freq, **my_rrule_rec))
                for dt in rr:
                    clone = copy.copy(t)
                    clone.pop('rrule')
                    clone['when'] = dt.isoformat()
                    transactions.insert(0, clone)
                continue
            if t_type == 'nested':
                when = determine_when(t)
                to_add = self._mk_trans_log(t['subs'], default_when=when)
            elif t_type in ('trans', 'checkpoint'):
                when = determine_when(t)
                t_rec = copy.copy(t)
                t_rec.update(when=when)
                to_add = [t_rec]
            elif t_type == 'import':
                to_add = self._mk_trans_log(
                    getattr(self, '_importer_' + t['importer'])(t)
                )
            flattened += to_add
            i += len(to_add)

        return flattened

    def fill_bal_log(self):
        trans_log = self.trans_log
        accounts = self.accounts
        bals = defaultdict(lambda: 0)
        bal_log = []
        def do_one_side(acct, amt):
            #do it for both the real account and the sub account
            accts = [acct] if not '/' in acct else [acct, acct.split('/')[0]]
            for a in accts:
                bals[a] += amt
                bal_log.append(dict(acct=a, when=when, bal=bals[a]))

        for aid, body in accounts.items():
            bals[aid] = body.get('bal', 0)
        for t in trans_log:
            when = t['when']
            typ = t.get('type', 'trans')
            if typ == 'trans':
                if 'src' in t and t['src'] != None:
                    do_one_side(t['src'], float(t['amt']))
                if 'dest' in t and t['dest'] != None:
                    do_one_side(t['dest'], float(t['amt']))
            elif typ == 'checkpoint':
                #TODO: checkpoints only affect real accounts
                bals[t['acct']] = float(t['amt'])
                bal_log.append(dict(acct=t['acct'], when=when, bal=bals[t['acct']]))
        self.bal_log += bal_log

    def fill_bal_log_by_acct_id(self):
        keyed = defaultdict(list)
        for rec in self.bal_log:
            myrec = copy.copy(rec)
            acct = rec.pop('acct')
            keyed[acct].append(rec)
        self.bal_log_by_acct_id = keyed

    def _importer_rbc(self, trans):
        assert trans['importer'] == 'rbc'
        def acct_match(rec):
            a_type = rec['Account Type']
            a_number = rec['Account Number']
            if a_type in ('Chequing', 'Savings'):
                uri = 'bankacct:ca.{}'.format(a_number)
            elif a_type == 'Visa':
                uri = 'visa:{}'.format(a_number)
            elif a_type == 'MasterCard':
                uri = 'mastercard:{}'.format(a_number)
            else:
                self.log.debug('Couldn\'t match account due to unknown type: {}'.format(rec))
                return None
            for k, v in self.accounts.items():
                if 'id' in v and v['id'] == uri:
                    return k
        def dest_match(rec):
            #TODO
            self.log.debug('Couldn\'t match dest: {}'.format(rec))
            return None
        srcfn = trans['src']

        with open(srcfn) as srcfile:
            csvr = csv.DictReader(srcfile)
            ret = []
            for rec in csvr:
                try:
                    src = acct_match(rec)
                    dest = dest_match(rec)
                    #TODO: handle different currencies
                    amt = -float(rec['CAD$'])
                    desc = '{}\n{}'.format(rec['Description 1'], rec['Description 2'])
                except Exception as exc:
                    raise
                    self.log.debug('skipped bad rec "{}" due to exc: {}'.format(rec, exc))
                    continue
                ret.append(dict(src=src, amt=amt, dest=dest,
                                when=rec['Transaction Date']))
            lvl = (logging.WARN if len(ret) == 0 else logging.INFO)
            self.log.log(lvl, 'rbc importer found {} transactions'.format(len(ret)))
            return ret

    def plot(self, include='*', exclude=[]):
        for k, bal_log in self.bal_log_by_acct_id.items():
            if include == '*' or (k in include and k not in exclude):
                vals_x = [rec['when'] for rec in bal_log]
                vals_y = [rec['bal'] for rec in bal_log]
                line, = plt.plot(vals_x, vals_y, label=k)
        plt.ylabel('CAD$')
        plt.legend()
        plt.show()

if __name__ == '__main__':
    logging.basicConfig()
    log = logging.getLogger('')
    log.setLevel(logging.INFO)
    logging.getLogger('FinDb').setLevel(logging.DEBUG)
    log.info('reading from stdin...')
    data_in = json.load(sys.stdin)
    db = FinDb(data_in)
    db.plot()
