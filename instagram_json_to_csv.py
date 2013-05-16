#!/usr/bin/env python
""" Quick convert to JSON to CSV for instagram data

NOTE: this is a quick hack to make instagram data from an
      existing source file.
"""
import os
import sys
import json
import datetime
from csv import DictWriter, QUOTE_ALL
from codecs import open

# want to make sure that we have the encoding set correctly to deal with WEB data.
reload(sys)
sys.setdefaultencoding("utf-8")

DATE_TIME_MASK = '%m/%d/%Y %H:%M:%S'
IGNORES = ['tags']
DATA_KEY = 'data'
TRANSFORMS = {
    # transforms are:
    # tag to be written:
    #   inputs are key and value
    #   check is on key == something, if not return None
    #   else return value item
    'username': lambda k,v: v['username'] if k == 'user' else None,
    'comment_count': lambda k,v: v['count'] if k == 'comments' else None,
    'images_low_resolution': lambda k,v: v['low_resolution']['url'] if k == 'images' else None,
    'images_standard_resolution': lambda k,v: v['standard_resolution']['url'] if k == 'images' else None,
    'likes_count': lambda k,v: v['count'] if k == 'likes' else None,
    'created_time_datetime': lambda k,v: datetime.datetime.fromtimestamp(float(v)) if k == 'created_time' else None,
    'caption_created_time_datetime': lambda k,v: datetime.datetime.fromtimestamp(float(v['created_time'])) if k == 'caption' else None,
    'caption_text': lambda k,v: v['text'] if k == 'caption' else None,
    'latitude': lambda k,v: v['latitude'] if k == 'location' else None,
    'longitude': lambda k,v: v['longitude'] if k == 'location' else None,
    'location_name': lambda k,v: v['name'] if k == 'location' else None,
    'users_in_photo_count': lambda k,v: len(v) if k == 'users_in_photo' else None,
}

VALUE_TRANSFORMS = {
    # after we transform we might want to change the data, or at least clean it up
    'created_time_datetime': lambda v: v.strftime(DATE_TIME_MASK) if type(v) is datetime.datetime else v,
    'caption_created_time_datetime': lambda v: v.strftime(DATE_TIME_MASK) if type(v) is datetime.datetime else v,
    'users_in_photo_count': lambda v: 0 if v == None else v
}


def main(file_to_conv, output_file=None):
    # name output file to input file if not defined
    if output_file is None:
        output_file = os.path.splitext(file_to_conv)[0] + '.csv'

    print "Opening:", file_to_conv
    input_file = open(file_to_conv, 'rb').readlines()
    src = []
    # make sure that we trim the json file
    for i, line in enumerate(input_file):
        if line.strip() == '{':
            src = input_file[i:]
            break
    print "Loading %s as JSON" % file_to_conv
    src = json.loads('\n'.join(src))

    data = src[DATA_KEY]
    print "Flattening the data"

    data = flatten_results(data)

    print "Writing to CSV:", output_file
    first_row = data.next()
    headers = first_row.keys()

    csv_file = DictWriter(open(output_file, 'wb', encoding='utf-8'),delimiter=',', quotechar='"', quoting=QUOTE_ALL, fieldnames=headers)
    csv_file.writeheader()
    csv_file.writerow(first_row)
    for row in data:
        csv_file.writerow(row)
    print "File written:", output_file

def flatten_results(data, transforms=TRANSFORMS, ignores=IGNORES, value_transforms=VALUE_TRANSFORMS):
    """ Convert the JSON results into flattened list so that
        we can publish to a CSV file.
    """
    skip_keys = []
    for row in data:
        new_row = {}
        for k,v in row.items():
            if type(v) is str:
                v = v.replace('"',"'")
            if k.lower() in ignores: continue
            for new_key, lmd in transforms.items():
                if v is None: continue
                lmd = lmd(k,v)
                if lmd is None: continue
                new_row[new_key] = lmd
                if not k in skip_keys: skip_keys.append(k)
            # make sure we have all the rows
            for key in transforms.keys():
                if key in new_row.keys(): continue
                new_row[key] = None

            # make sure we transform the data types into what we want
            for key, trans in value_transforms.items():
                new_row[key] = trans(new_row[key])

            if k in skip_keys: continue
            if not new_row.has_key(k): new_row[k] = v

        yield new_row


if __name__ == "__main__":
    argv = sys.argv[1:]
    if len(argv) < 1:
        print "usage: convert_to_csv JSON_FILE [OUPUT_FILE]"
        exit()
    input_file = argv.pop(0)
    output_file = None

    if len(argv) == 1:
        output_file = argv.pop(0)
    main(input_file, output_file)
