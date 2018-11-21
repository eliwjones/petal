"""
  See: https://github.com/eliwjones/petal/blob/master/processor.py
"""

from decimal import Decimal

import csv
import glob
import os
import re




def process(fileglob):
    """
      Glob up files and process them one by one.
    """

    filepaths = glob.glob(fileglob)

    for filepath in filepaths:
        datum_list = []
        aggregated_data = {'user_id': None, 'n': 0, 'sum': 0, 'min': 0, 'max': 0}

        for parsed_row in extract_csv_data(filepath):

            if aggregated_data['user_id'] is None:
                aggregated_data['user_id'] = parsed_row['user_id']

            if aggregated_data['user_id'] != parsed_row['user_id']:
                # We want earliest 'date' datum first.
                sorted_datum = sorted(datum_list, key=lambda k: k['date'])

                for datum in sorted_datum:
                    aggregated_data = update_aggregated_data(aggregated_data, datum)

                # Dump current stack of user info to output file.
                dump_aggregated_data(aggregated_data, output_filepath(filepath))

                # Re-initialize
                datum_list = []
                aggregated_data = {'user_id': parsed_row['user_id'], 'n': 0, 'sum': 0, 'min': 0, 'max': 0}

            """
              We are still on same user_id so just append to datum_list.
            """
            datum_list.append(parsed_row)


        """
          At end of csv file, roll-up and dump last chunk of user_data.
        """

        sorted_datum = sorted(datum_list, key=lambda k: k['date'])

        for datum in sorted_datum:
            aggregated_data = update_aggregated_data(aggregated_data, datum)

        dump_aggregated_data(aggregated_data, output_filepath(filepath))





def dump_aggregated_data(aggregated_data, output_filepath):
    header = ('user_id','n','sum','min','max')
    write_header = False

    if not os.path.exists(output_filepath):
        write_header = True  # Feels dumb.

    with open(output_filepath, 'a+') as f:
        csv_writer = csv.DictWriter(f, delimiter='|', fieldnames=header, extrasaction='ignore')

        if write_header:
            csv_writer.writeheader()

        csv_writer.writerow(aggregated_data)


def update_aggregated_data(aggregated_data, datum):
    """
      Assumes datum has already been sorted by date.

      * inc transaction count, adjust sum
      * adjust min and max amounts based off of new sum.

      datum ~ user_id, amount, desc, date, type, misc
        type = ['debit', 'credit']
        date = 'YYYY-MM-DD'

    """
    if 'last_date' not in aggregated_data:
        aggregated_data['last_date'] = datum['date']

    if aggregated_data['last_date'] != datum['date']:
        """
          We are calculating daily min, max values so only update when hit new date.
        """

        if aggregated_data['sum'] < aggregated_data['min']:
            aggregated_data['min'] = aggregated_data['sum']

        if aggregated_data['sum'] > aggregated_data['max']:
            aggregated_data['max'] = aggregated_data['sum']

        aggregated_data['last_date'] = datum['date']
    


    sign = 1
    if datum['type'] == 'debit':
        sign = -1

    aggregated_data['n'] += 1
    aggregated_data['sum'] += sign * Decimal(datum['amount'])

    return aggregated_data


def extract_csv_data(filepath, delimiter='|'):
    """
      Use generators to save memory.
    """

    with open(filepath, 'r') as f:
        reader = csv.DictReader(f, delimiter=delimiter, escapechar='\\')
        for row in reader:
            yield row


def output_filepath(filepath):
    nums = re.findall('\d+', filepath)
    num = ''
    if nums:
        num = nums[0]

    return "aggregated-transactions%s.csv" % (num)


if __name__ == '__main__':
    """
      Ungzip all the csvs, overwrite always.  What could go wrong?
    """
    from subprocess import Popen
    Popen('gzip -fkd transactions*csv.gz', shell=True).wait() # Sorta funky, but works for me.

    process('transactions*.csv')
