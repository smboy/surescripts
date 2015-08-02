#!/usr/bin/env python
import ConfigParser
import MySQLdb
import sys
import logging as log

log.basicConfig(
    level=log.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def get_params(section):
    """ returns a json with the connection parameters """
    config = ConfigParser.ConfigParser()
    filename = "config.ini"
    config.read(filename)
    db = {}
    if config.has_section(section):
        items = config.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception(
            '{0} not found in the {1} file'.format(section, filename))
    return db


def connect(db):
    db_config = get_params(db)
    try:
        log.info('Connecting to MySQL database...')
        conn = MySQLdb.connect(host=db_config['host'], user=db_config['user'], passwd=db_config['password'],
                               db=db_config['database'], local_infile=1)
        with conn:
            log.info('connected to %s' % db)

    except Exception, e:
        raise
        send_mail("Failed to connect to Mysql %s" % db)
        sys.exit(1)

    return conn


def close_connection(conn, cursor):
    cursor.close()
    conn.close()


def send_mail(_sub):
    import smtplib
    from email.mime.text import MIMEText

    fromEmail = get_params("emails")["from_email"]
    toEmail = get_params("emails")["to_email"]
    content = """\
%s
\n 
----------
""" % _sub
    msg = MIMEText(content, 'plain')
    msg['From'] = fromEmail
    msg['To'] = toEmail
    msg['Subject'] = _sub
    server = smtplib.SMTP('localhost')
    try:
        server.sendmail(fromEmail, toEmail,msg.as_string())
    except Exception, exc:
        sys.exit( "Failed to send email ; %s" % str(exc) )
