#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import datetime
import requests
from lxml import etree, objectify
import logging as log
import connections
import sys
from random import randint
import zipfile

reload(sys)
sys.setdefaultencoding('utf8')

try:
    from collections import OrderedDict
except ImportError:
    # python 2.6 or earlier, use backport
    from ordereddict import OrderedDict

log.basicConfig(
    level=log.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def db_dateformat(str):
    if str is not None:
        # date format from file: 2015-02-05T00:00:00.01Z"
        return str.replace("T", " ").split(".")[0]
    else:
        return None


def read_zip_file(filepath):
    zfile = zipfile.ZipFile(filepath)
    for finfo in zfile.infolist():
        ifile = zfile.open(finfo)
        return ifile


fields = ["NCPDPID", "StoreNumber", "Organization_name", "AddressLine1", "AddressLine2", "City", "State", "Zip",
          "StandardizedAddressLine1", "StandardizedAddressLine2", "StandardizedCity", "StandardizedState",
          "StandardizedZip", "PhonePrimary", "Fax", "Email", "AlternatePhoneNumbers", "ActiveStartTime",
          "ActiveEndTime", "ServiceLevel", "PartnerAccount", "LastModifiedDate", "CrossStreet", "RecordChange",
          "OldServiceLevel", "Version", "NPI", "SpecialtyType", "FileID", "StateLicenseNumber", "MedicareNumber",
          "MedicaidNumber", "PPONumber", "PayerID", "BINLocationNumber", "DEANumber", "HIN", "SecondaryCoverage",
          "NAICCode", "PromotionNumber", "SocialSecurity", "PriorAuthorization", "MutuallyDefined", "DirectAddress",
          "OrganizationType", "OrganizationID", "ParentOrganizationID", "Latitude", "Longitude", "Precise", "UseCases"]
reqd_fields = ["NCPDPID", "StoreNumber", "Organization_name", "AddressLine1", "AddressLine2", "City", "State", "Zip",
               "PhonePrimary", "Fax", "ActiveStartTime", "ActiveEndTime", "ServiceLevel", "PartnerAccount",
               "LastModifiedDate", "NPI", "Latitude", "Longitude"]

ss_params = connections.get_params("surescript")

os.environ['http_proxy'] = ss_params['proxy_server']
os.environ['https_proxy'] = ss_params['proxy_server']


def get_pass_hash():
    """
    Java code
        String password = "password";
        sun.misc.BASE64Encoder encoder = new sun.misc.BASE64Encoder();
        String encoded =
        encoder.encode(MessageDigest.getInstance("SHA1").digest(password.toUpperCase().getBytes ("UTF-16LE")));
        System.out.println("encoded password: " + encoded);
    """
    import base64
    import hashlib
    # Base64 encoded, SHA1 hash of the UTF-16 encoded
    pwd = base64.b64encode(hashlib.sha1(ss_params['pwd'].encode('utf-16-le').upper()).digest())
    return pwd


dt = '{0}'.format(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%dT%H:%M:%S'))
created_at = "%s.0Z" % dt
msg_id = 'MSGID%s' % dt

content = """<?xml version="1.0" encoding="UTF-8"?>
<Message xmlns="http://www.surescripts.com/messaging" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="004" release="006" xsi:schemaLocation="http://www.surescripts.com/messaging SS_Directories_4.6_External.xsd">
    <Header>
        <To>{MAILTO}</To>
        <From>{MAILFROM}</From>
        <MessageID>{msg_id}</MessageID>
        <SentTime>{dt}</SentTime>
        <Security>
            <UsernameToken>
            <Username>{USERNAME}</Username>
            <Password>{pwd}</Password>
            <Nonce>{random_int}</Nonce>
            <Created>{dt}</Created>
            </UsernameToken>
        </Security>
    </Header>
    <Body>
        <DirectoryDownload>
            <AccountID>{ACCID}</AccountID>
            <VersionID>{VER}</VersionID>
            <Taxonomy>
                <TaxonomyCode>{TAXCD}</TaxonomyCode>
            </Taxonomy>
        </DirectoryDownload>
    </Body>
</Message>"""

payload = content.format(MAILTO=ss_params['mailto'], MAILFROM=ss_params['mailfrom'],
                         USERNAME=ss_params['username'], dt=created_at, msg_id=msg_id,
                         pwd=get_pass_hash(), random_int=randint(10, 20),
                         ACCID=ss_params['accountid'], VER=ss_params['versionid'], TAXCD=ss_params['taxonomy_code'])
log.info('XML payload required for SureScripts:')
log.info(payload)
headers = {'Content-Type': 'application/xml'}
try:
    xml_response = requests.post('https://staging.surescripts.net/Directory4dot6/directoryxmlserver.aspx', data=payload,
                                 headers=headers, timeout=(10.0, 1.0))
    log.info('XML Response from SureScripts:')
    log.info(str(xml_response.text))
except:
    raise
    connections.send_mail("ERROR: Failed to download SureScripts data")
    sys.exit(1)

log.info('Parsing XML response from SureScripts to extract Download URL')
root = objectify.fromstring(str(xml_response.text))
try:
    response_zip_file = root.Body.DirectoryDownloadResponse.URL
except:
    raise
    log.error('URL tag is not found in the download url')
    connections.send_mail("ERROR: Failed to download SureScripts data")
    sys.exit(1)
download_url = "https://staging.surescripts.net/Downloads/%s" % response_zip_file
log.info('Download url: %s' % download_url)
try:
    r = requests.get(download_url, headers=headers)
except:
    raise
    connections.send_mail("ERROR: Failed to download SureScripts data")
    sys.exit(1)

# [ zip file write process ]
# If we donot want to write directly to database, then remove the complete code below

filename = "%s" % str(response_zip_file)
log.info('Processing file: %s to extract required information' % filename)
open(filename, 'wb').write(r.content)
cnt = 0
sz = len(fields)
db_rec = []
for row in read_zip_file(filename):
    data = row.strip().split("|")
    cnt += 1
    d = OrderedDict()
    # convert data to json so that data can be referred as their key names
    for i in range(sz):
        d[fields[i]] = data[i]

    d['State'] = d['State'].upper()
    d['Zip'] = d['Zip'][:5]
    dates = ["ActiveStartTime", "ActiveEndTime", "LastModifiedDate"]
    for dt in dates:
        d[dt] = db_dateformat(d[dt])
    d['PhonePrimary'] = d['PhonePrimary'][:10]

    db_fields = tuple([None if d[r] is None or d[r] == '' else d[r] for r in reqd_fields])
    db_rec.append(db_fields)

log.info('Total Rows to be inserted into DB: %s rows' % cnt)
log.info('Starting MySQL load...')

load_sql = "insert into stg_ss_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
conn = connections.connect("database")
with conn:
    cursor = conn.cursor()
    log.info('Truncating Staging table: stg_ss_data..')
    cursor.execute("TRUNCATE stg_ss_data")
    log.info('Table stg_ss_data truncated')
    cursor.executemany(load_sql, db_rec)

log.info('Completed MySql load.')
connections.send_mail("SUCCESS: Completed surescripts data download")
