#!/usr/bin/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import argparse
import re
import datetime
import operator
import pprint
import glob
import gzip

slow_threshold = 10  #seconds

# Nothing to change past here
verbose = None
re_slow = re.compile(r'^(\d+-\d+-\d+\s+\d+:\d+:\d+\.\d+)\s+\w+\s+0.*slow.*(client\.\d+\.\d+:\d+).*from\s+(\d+(,\d+)*)')
re_io = re.compile(r'^(\d+-\d+-\d+\s+\d+:\d+:\d+\.\d+)\s+\w+\s+1.*<==.*(osd\.\d+|client).*(client\.\d+\.\d+:\d+).*')

def get_date(datestring):
    nofrag, frag = datestring.split(".")
    date = datetime.datetime.strptime(nofrag, "%Y-%m-%d %H:%M:%S")
    frag = frag[:6]  #truncate to microseconds
    frag += (6 - len(frag)) * '0'
    date = date.replace(microsecond=int(frag))
    return date

def get_log_files(args):
    if args.all is True:
        if args.zip is True:
            return glob.glob(args.logdir + "ceph-osd.*.log*")
        else:
            return glob.glob(args.logdir + "ceph-osd.*.log")
    else:
        if args.zip is True:
            return glob.glob(args.logdir + "ceph-osd." + str(args.osd) + ".log*")
        else:
            return glob.glob(args.logdir + "ceph-osd." + str(args.osd) + ".log")
            

def find_blocked(args):
    slow_osds = {}
    if args.all is True:
        if verbose >= 1:
            print "Searching all OSDs."
        for file in get_log_files(args):
            result = search_logs(file)
            if result:
                slow_osds.update(result)
        pass
    else:
        if verbose >= 1:
            print "Going to search OSD " + str(args.osd) + "."
        slow_osds = search_logs(get_log_files(args)[0])
    if verbose >=3:
        pprint.pprint(slow_osds)
    if len(slow_osds) > 0:
        print_output(slow_osds)
    else:
        print "Could not find any slow OSDs."

def print_output(slow_osds):
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

    osd_report = sorted(osd_report.items(), key=operator.itemgetter(1))
    if len(osd_report) > 0:
        for i in osd_report:
            print "OSD " + str(i[0]) + ": " + str(i[1])
    else:
        print "Could not find any slow OSDs."

def search_logs(logfile):
    if verbose >= 1:
        print "Searching through " + logfile + "..."
    try:
        # Iterate through the file looking for slow messages so we know
        # which I/O are problematic
        if 'gz' in logfile:
            with gzip.open(logfile, 'rb') as f:
                return scan_file(f)
        else:
            with open(logfile, 'rb') as f:
                return scan_file(f)

        return None

    except OSError, e:
        print "Could not open " + logfile + " for reading."
        sys.exit(1)

def scan_file(fd):
    slow_osds = {}
    # If the line has slow, capture the date/time, the client id
    # and the secondary OSDs as slow clients
    for line in fd:
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
        fd.seek(0)
        for line in fd:
            matches = re_io.match(line)
            if matches and matches.group(3) in slow_osds.keys():
                if 'client' in matches.group(2):
                    slow_osds[matches.group(3)]['start'] = get_date(matches.group(1))
                elif 'osd' in matches.group(2) and slow_osds[matches.group(3)].get('start', None):
                    latency = get_date(matches.group(1)) - slow_osds[matches.group(3)]['start']
                    osd = matches.group(2).split(".")[1]
                    if latency < datetime.timedelta(seconds=slow_threshold):
                        if osd in slow_osds[matches.group(3)]['slow']:
                            slow_osds[matches.group(3)]['slow'].remove(osd)
                        if not slow_osds[matches.group(3)].get('fast', None):
                            slow_osds[matches.group(3)]['fast'] = [osd]
                        elif osd not in slow_osds[matches.group(3)]['fast']:
                            slow_osds[matches.group(3)]['fast'] += [osd]
        return slow_osds
        

def main():
    # Main execution
    global verbose
    parser = argparse.ArgumentParser(description="Hunts for slow OSDs by looking thorugh OSD logs.")
    osdgroup = parser.add_mutually_exclusive_group(required=True)
    osdgroup.add_argument('-o', '--osd', type=int, help="an OSD on this host that is reporting slow I/O.")
    osdgroup.add_argument('-a', '--all', action="store_true", default="false", help="Search logs of all OSDs in logdir.")
    parser.add_argument('-z', '--zip', action="store_true", default="false", help="Also search through compressed logfiles.")
    parser.add_argument('-l', '--logdir', default="/var/log/ceph/", help="Location of log files. Defaults to /var/log/ceph/.")
    parser.add_argument('-v', '--verbose', action="count", default=0, help="Increase verbosity, more flags means more output.")
    args = parser.parse_args()
    verbose = args.verbose
    if verbose >= 3:
        pprint.pprint(args)
    if args.all or args.osd:
        find_blocked(args)

if __name__ == "__main__":
    main()
