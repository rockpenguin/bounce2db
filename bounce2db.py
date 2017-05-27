#!/usr/bin/env python

import datetime
import _mssql
import optparse
import os
import re
import string
import sys

# Let's setup the DB connection
dbcxn = _mssql.connect(server='***.***.***.***', user='*******', password='*******', database='********')
dbcxn.debug_queries = 0 #change to 1 to enable query debugging

# Now let's setup the command line options
cmdopts = optparse.OptionParser("usage: %prog [options] /path/to/Maildir/[cur][new] > output_file")
cmdopts.add_option("-D", "--delete", action="store_true", dest="delfile", default=False, help="Delete the email file after extracting info")

cmdopts.disable_interspersed_args()
opts, args = cmdopts.parse_args()

if len(args) != 1:
    cmdopts.error("Sorry, you'll have to speak up. You must supply a path to the folder containing the Maildir messages. Try -h OR --help")

## OK, now the fun begins! ##

# let's make sure that the directory supplied by the user is an actual directory, and that it ends with /
if os.path.isdir(args[0]):
    if args[0].endswith('/'):
        maildir = args[0]
    else:
        maildir = args[0] + '/'

walk_counter = 0
for root,dirs,files in os.walk(maildir):
    # the os.walk function returns EVERYTHING in the path, but we only want the
    # filenames, so we get the first tuple and then we quit the loop
    walk_counter+=1
    if walk_counter > 1: break

    tick = 0
    for filename in files:
        tick += 1
        # if tick == 1000: break # only doing 1000 at a time per the Online guys request

        if os.path.isdir(filename) or os.path.islink(filename): continue

        mailtxt = ''

        # open the mail message and read it in
        mailfile = open(root + filename)
        mailtxt = mailfile.read(-1)
        mailfile.close()

        # check to see if the status code is in the 500's (e.g. 5.0.0); if the regexp doesn't find a match
        # then we'll boogie on to the next email message
        smtpcode = re.search(r'(?im)^(Status:\s*)(5\.[0-9]\.[0-9])', mailtxt)
        if smtpcode == None:
            continue
        else:
            smtpcode = 'bouncemail smtp: %s' % smtpcode.group(2)

        # use regexps to extract both the original and final recipients, as usually the original recipient is what we're after
        # but sometimes the only email address in the message is the final recipient
        orig_recip = re.search(r'(?im)^(Original-Recipient:)(\s*rfc822;\s*)([0-9a-zA-Z#_&%=~\.\-\$\\*\+\^]+@[0-9a-zA-Z\.\-]+$)', mailtxt)
        final_recip = re.search(r'(?im)^(Final-Recipient:)(\s*rfc822;\s*)([0-9a-zA-Z#_&%=~\.\-\$\\*\+\^]+@[0-9a-zA-Z\.\-]+$)', mailtxt)

        if orig_recip:
            email = orig_recip.group(3)
        elif final_recip:
            email = final_recip.group(3)

        # if both recip values are None, then bail on this loop iteration
        if (orig_recip == None) and (final_recip == None): continue

        # generate the SQL
        sql = 'DECLARE @result int ' + \
              'EXEC @result = dbo.proc_bounce_update_acct_status @email=%s, @smtpcode=%s ' + \
              'SELECT @result'

        # we should only get back a result (1=success, 0=fail, 2=query failed)
        try:
            spresult = dbcxn.execute_scalar(sql, (email,smtpcode))
        except:
            spresult = 2

        # output the results for logging
        print '%s\t%s\tsmtp_code=%s\taccount_flagged=%s' % (datetime.date.today(), email, smtpcode, spresult)

        # now let's remove the message file
        if opts.delfile:
          os.remove(root + filename)

dbcxn.close()

sys.exit()
