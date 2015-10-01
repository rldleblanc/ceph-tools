#!/usr/bin/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import argparse
import re
import datetime
import operator
import pprint

log_loc = "/var/log/ceph/"
slow_threshold = 10  #seconds

# Nothing to change past here
args = None
re_slow = re.compile(r'^(\d+-\d+-\d+\s+\d+:\d+:\d+\.\d+)\s+\w+\s+0.*slow.*(client\.\d+\.\d+:\d+).*from\s+(\d+(,\d+)*)')
re_io = re.compile(r'^(\d+-\d+-\d+\s+\d+:\d+:\d+\.\d+)\s+\w+\s+1.*<==.*(osd\.\d+|client).*(client\.\d+\.\d+:\d+).*')

def get_date(datestring):
    nofrag, frag = datestring.split(".")
    date = datetime.datetime.strptime(nofrag, "%Y-%m-%d %H:%M:%S")
    frag = frag[:6]  #truncate to microseconds
    frag += (6 - len(frag)) * '0'
    date = date.replace(microsecond=int(frag))
    return date

def read_log():
    slow_osds = {}
    logfile = log_loc + 'ceph-osd.' + str(args.osd) + '.log'
    try:
        # Iterate through the file looking for slow messages so we know
        # which I/O are problematic
        with open(logfile, 'rb') as f:
            # If the line has slow, capture the date/time, the client id
            # and the secondary OSDs as slow clients
            for line in f:
                matches = re_slow.match(line)
                if matches and not matches.group(1) in slow_osds.keys():
                    slow_osds[matches.group(2)] = {}
                    #slow_osds[matches.group(2)]['start'] = get_date(matches.group(1))
                    slow_osds[matches.group(2)]['slow'] = matches.group(3).split(",")

            # On the second iteration, look for lines that have the client id
                # 1. Get the data/time stamp from the request from the client,
                #    set as the start time for the I/O
                # 2. If it has ondisk status. Get the date/time. Compare with the
                #    start time and if less than 30 seconds, move osd to the
                #    fast list.

            if len(slow_osds) > 0:
                # Jump back to the start of the file
                f.seek(0)
                for line in f:
                    matches = re_io.match(line)
                    if matches and matches.group(3) in slow_osds.keys():
                        if 'client' in matches.group(2):
                            slow_osds[matches.group(3)]['start'] = get_date(matches.group(1))
                        elif 'osd' in matches.group(2):
                            latency = get_date(matches.group(1)) - slow_osds[matches.group(3)]['start']
                            osd = matches.group(2).split(".")[1]
                            if latency < datetime.timedelta(seconds=slow_threshold):
                                if osd in slow_osds[matches.group(3)]['slow']:
                                    slow_osds[matches.group(3)]['slow'].remove(osd)
                                if not slow_osds[matches.group(3)].get('fast', None):
                                    slow_osds[matches.group(3)]['fast'] = [osd]
                                elif osd not in slow_osds[matches.group(3)]['fast']:
                                    slow_osds[matches.group(3)]['fast'] += [osd]
                
                # Tally up the slow OSDs
                    # go thorugh all arrays and create a new array of slow OSDs
                    # with the OSD ID as the key and increment the value for each
                    # Sort the list asending and print out the OSDs. 
    
                osd_report = {}
                for key in slow_osds.keys():
                    if slow_osds[key].get('start', None):
                        if slow_osds[key].get('slow', None):
                            for i in slow_osds[key]['slow']:
                                if i not in osd_report.keys():
                                    osd_report[i] = 1
                                else:
                                    osd_report[i] += 1
    
                sorted_osd_report = sorted(osd_report.items(), key=operator.itemgetter(1))
                for i in sorted_osd_report:
                    print "OSD " + str(i[0]) + ": " + str(i[1])
            else:
                print "No OSDs with latency greater than " + str(slow_threshold) + " seconds found on osd " + str(args.osd) + "."
        
    except OSError, e:
        print "Could not open " + logfile + " for reading."
        sys.exit(1)

def main():
    # Main execution
    global args
    parser = argparse.ArgumentParser(description="Hunts for slow OSDs by looking thorugh OSD logs.")
    parser.add_argument('osd', type=int, help="an OSD on this host that is reporting slow I/O.")
    args = parser.parse_args()
    read_log()

if __name__ == "__main__":
    main()
