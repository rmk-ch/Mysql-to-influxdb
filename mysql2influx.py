#!/usr/bin/python

import logging
import argparse
import pymysql
import pymysql.cursors
import time

from configparser import RawConfigParser
from influxdb_client import InfluxDBClient
from influxdb_client .client.write_api import SYNCHRONOUS
from datetime import datetime
logger = logging.getLogger(__name__)


class Mysql2Influx:

    def __init__(self,config):

        #TODO put site info into settings file
        self._site_name = config.get('site_info','site_name')
        self._table = config.get('mysql','table')
        self._time_field = config.get('mysql','time_field')

        #intitialise client for mysql database
        self._mysql_host = config.get('mysql','host')
        self._mysql_username = config.get('mysql','username')
        self._mysql_password = config.get('mysql','password')
        self._mysql_db = config.get('mysql','db')
        self._extra_where_field = config.get('mysql', 'extra_where_field')

        self._influx_db_url = config.get('influx','url')
        self._influx_db_token = config.get('influx','token')
        self._influx_db_org = config.get('influx','org')
        self._influx_db_bucket = config.get('influx','bucket')

        self._check_field = config.get('mysql','check_field')

        self.connect()


    def connect(self):
        self._db_client = pymysql.connect ( host = self._mysql_host,
                                            user = self._mysql_username,
                                            password = self._mysql_password,
                                            db = self._mysql_db,
                                            cursorclass = pymysql.cursors.DictCursor
                                            )

        self._influx_client = InfluxDBClient(
                                            url=self._influx_db_url,
                                            token=self._influx_db_token,
                                            org=self._influx_db_org,
                                            debug=False
                                            )
        self._write_api = self._influx_client.write_api(write_options=SYNCHRONOUS)



    def transfer(self, flushBefore = False):
        if flushBefore:
            self._resetMysql()

        mapSourceTarget = dict()
        mapSourceTarget['5'] = ('temperature', 'HennenstallInnen', 'value')

        for source, target in mapSourceTarget.items():
            
            if flushBefore:
                logger.debug('Flushing old entries in influxdb')
                delete_api = self._influx_client.delete_api()
                delete_api.delete(start = "1970-01-01T00:00:00Z", stop = "2021-02-01T00:00:00Z", predicate=f'_measurement="{target[0]}"', bucket=self._influx_db_bucket, org=self._influx_db_org)


            rows = self._getDataFromMysql(source)
            influxData = self._convertToInflux(rows, measurement=target[0], location=target[1], field=target[2])
            if len(rows) < 1:
                logger.info('No data to be transferred')
                continue

            self._writeToInflux(influxData)
            self._updateEntriesInMysql(rows)

        logger.debug('All data transfers completed')


    def _resetMysql(self):
        """
        Once the data is configured and within influx we can pruge our database
        """
        query = f"UPDATE {self._table} SET {self._check_field}=0;"
        logger.debug('Resetting mysql source : executing query %s '% query)
        c =  self._db_client.cursor()
        c.execute(query)
        self._db_client.commit()


    def _getDataFromMysql(self, sourceId):
        """
        get the cursor to dump all the data from mysql
        """
        query = "SELECT * FROM `%s` WHERE `%s`=0 AND `%s`=%s ORDER BY %s DESC"%(self._table,self._check_field,self._extra_where_field, sourceId, self._time_field)

        logger.debug('executing query %s '% query)
        cursor = self._db_client.cursor()
        cursor.execute(query)

        # pull data from mysql in X increments
        rows = cursor.fetchall()
        logger.info('querying MYSQL got %s rows'%len(rows))

        return rows


    def _writeToInflux(self,data_point : list, flushBefore=False):
        """
        Break up data to make sure in the format the inflxu like
        """
        logger.debug('Sending data to influx %s ...'%data_point[0])
        self._write_api.write(bucket=self._influx_db_bucket, record=data_point)


    def _convertToInflux(self, data : list, measurement : str, field : str, location : str) -> list:
        data_list = list()

        if data:
            logger.debug('Got data from mysql')
            for row in data:

                data_point = {
                    "measurement" : measurement,
                    "tags": {
                        "location": location
                    },
                    "time": "%s" % row[self._time_field].isoformat(),
                    "fields": {
                        field : row['measurement']
                    }
                }
                #logger.debug("data_point = %s"%data_point)
                data_list.append(data_point)

        return data_list

    def _updateEntriesInMysql(self, rows : list):
        ids = list()
        for entry in rows:
            ids.append(str(entry['id']))
        idsString = ",".join(ids)

        query = f"UPDATE {self._table} SET {self._check_field}=1  WHERE `id` IN ({idsString});"
        logger.debug('Updating rows : executing query %s '% query)
        c =  self._db_client.cursor()
        c.execute(query)
        self._db_client.commit()


def main():
    #Argument parsing
    parser = argparse.ArgumentParser(description = 'Get Time series data from MYSQL and push it to influxdb' )

    parser.add_argument( '-d', '--debug', help = 'set logging level to debug', action = 'store_true')
    parser.add_argument( '-c', '--config', help = 'config file location', nargs = 1, default = 'settings.ini' )
    parser.add_argument( '-s', '--server', help = 'run as server with interval ',action = 'store_true' )

    args = parser.parse_args()


    # Init logging
    logging.basicConfig(level=(logging.DEBUG if True or args.debug else logging.INFO))

    logger.debug('Starting up with config file  %s' % (args.config))
    #get config file
    config = RawConfigParser()
    config.read(args.config)

    _sleep_time = float(config.get('server','interval'))

    logger.debug('configs  %s' % (config.sections()))
    #start
    mclient = Mysql2Influx(config)
    if not args.server:
        mclient.transfer()
    else:
        logger.info('Starting up server mode interval:  %s' % _sleep_time)
        while True:
            try:
                mclient.transfer()
            except Exception as e:
                logger.exception("Error occured will try again")
            time.sleep(_sleep_time)
            mclient.connect()

if __name__ == '__main__':
    #Check our config file
    main()
